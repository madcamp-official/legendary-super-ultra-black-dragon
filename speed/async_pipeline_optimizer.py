"""
Async Pipeline Optimizer for 7-stage Pipeline Parallelism (PP=7, TP=1, DP=1).

Hides P2P activation-transfer latency (stage i -> i+1) behind compute of the
current microbatch, using a dedicated compute stream, a dedicated comm stream,
and CUDA events for cross-stream sync (no CPU-side torch.cuda.synchronize()
inside the hot loop).

Run with: torchrun --nproc_per_node=7 async_pipeline_optimizer.py
"""

import os
import time
from typing import List, Optional

import torch
import torch.distributed as dist
import torch.nn.functional as F


class AsyncPipelineOptimizer:
    """
    Holds per-rank state (streams, buffers, dummy weights) and implements
    both a blocking-sync pipeline and an async dual-stream overlap pipeline
    for benchmarking.
    """

    def __init__(
        self,
        rank: int,
        world_size: int,
        model_dim: int = 4096,
        micro_batch_size: int = 4,
        seq_len: int = 2048,
        device: Optional[torch.device] = None,
    ) -> None:
        self.rank = rank
        self.world_size = world_size
        self.model_dim = model_dim
        self.micro_batch_size = micro_batch_size
        self.seq_len = seq_len
        self.device = device if device is not None else torch.device(f"cuda:{rank}")

        self.tensor_shape = (self.micro_batch_size, self.seq_len, self.model_dim)

        # Two independent streams: compute never blocks waiting on comm and
        # vice versa unless we explicitly insert a wait_event.
        self.compute_stream: torch.cuda.Stream = torch.cuda.Stream(device=self.device)
        self.comm_stream: torch.cuda.Stream = torch.cuda.Stream(device=self.device)

        self.send_buffer: torch.Tensor = torch.randn(self.tensor_shape, device=self.device)
        self.recv_buffer: torch.Tensor = torch.zeros(self.tensor_shape, device=self.device)

        # Dummy weight standing in for one Qwen3-style transformer layer's
        # projection matrix (real layer would be attention + MLP block).
        self.weight: torch.Tensor = torch.randn((model_dim, model_dim), device=self.device)

    def dummy_compute_kernel(self, input_tensor: torch.Tensor) -> torch.Tensor:
        """Simulate a heavy transformer-layer forward: matmul + SiLU."""
        x = torch.matmul(input_tensor, self.weight)
        x = F.silu(x)
        return x

    def run_sync_pipeline(self, steps: int = 10) -> float:
        """
        Baseline: compute -> block -> blocking send/recv -> block.
        Downstream ranks sit idle while upstream compute+comm finishes
        serially, producing the classic pipeline bubble.
        """
        torch.cuda.synchronize(self.device)
        start_time = time.perf_counter()

        current_input = self.send_buffer.clone()

        for _ in range(steps):
            current_output = torch.matmul(current_input, self.weight)
            current_output = F.silu(current_output)
            torch.cuda.synchronize(self.device)  # wait for compute before touching network

            if self.rank < self.world_size - 1:
                dist.send(current_output, dst=self.rank + 1)  # blocking send
            if self.rank > 0:
                dist.recv(self.recv_buffer, src=self.rank - 1)  # blocking recv
                current_input = self.recv_buffer.clone()
            torch.cuda.synchronize(self.device)  # wait for comm before next compute

        end_time = time.perf_counter()
        return (end_time - start_time) / steps

    def run_async_pipeline(self, steps: int = 10) -> float:
        """
        Optimized: compute_stream and comm_stream run concurrently. The
        comm_stream only waits on a CUDA event (GPU-side), never on a CPU
        sync, so the host can keep issuing work for the next microbatch
        while the network transfer for the previous one is still in flight.
        """
        torch.cuda.synchronize(self.device)
        start_time = time.perf_counter()

        current_input = self.send_buffer.clone()
        current_output = self.send_buffer  # placeholder until step 0 fills it
        work_handles: List[dist.Work] = []

        # One event per step: records "compute for this step is done" so the
        # comm stream can gate its send on it without a CPU round-trip.
        compute_done_events: List[torch.cuda.Event] = [
            torch.cuda.Event() for _ in range(steps)
        ]

        for step in range(steps):
            # [Send previous step's output] Only after step 0 has produced
            # something. Issued on comm_stream, non-blocking (isend).
            if step > 0 and self.rank < self.world_size - 1:
                with torch.cuda.stream(self.comm_stream):
                    # comm_stream must not start moving bytes until the matmul/
                    # silu that produced current_output has actually finished
                    # on compute_stream -- that's what this wait_event enforces,
                    # entirely on the GPU timeline (no host block).
                    self.comm_stream.wait_event(compute_done_events[step - 1])
                    handle = dist.isend(current_output, dst=self.rank + 1)
                    work_handles.append(handle)

            # [Compute current step] Runs on compute_stream concurrently with
            # whatever the comm_stream is doing above/below.
            with torch.cuda.stream(self.compute_stream):
                current_output = self.dummy_compute_kernel(current_input)
                # Record completion so comm_stream can safely send this
                # exact tensor version next iteration (or below, for recv).
                compute_done_events[step].record(self.compute_stream)

            # [Receive next input] Non-blocking irecv on comm_stream. We do
            # NOT need to wait on compute here since recv_buffer is a
            # separate tensor from current_output -- no data hazard.
            if self.rank > 0:
                with torch.cuda.stream(self.comm_stream):
                    handle = dist.irecv(self.recv_buffer, src=self.rank - 1)
                    work_handles.append(handle)

            # current_input for the *next* iteration must wait until irecv
            # above has actually landed data in recv_buffer. We defer the
            # CPU-side wait to the top of next loop's compute via stream
            # ordering: compute_stream will wait on comm_stream through this
            # event so no torch.cuda.synchronize() is needed here.
            if self.rank > 0:
                recv_done_event = torch.cuda.Event()
                with torch.cuda.stream(self.comm_stream):
                    recv_done_event.record(self.comm_stream)
                self.compute_stream.wait_event(recv_done_event)
                current_input = self.recv_buffer.clone()
            else:
                current_input = current_output

        # Drain all outstanding isend/irecv handles -- required before
        # reusing send_buffer/recv_buffer or exiting, but done once at the
        # end rather than every step, so it doesn't reintroduce the bubble.
        for handle in work_handles:
            handle.wait()

        torch.cuda.synchronize(self.device)
        end_time = time.perf_counter()

        return (end_time - start_time) / steps


def main() -> None:
    try:
        dist.init_process_group(backend="nccl")
    except Exception as exc:  # noqa: BLE001 - surface init failures clearly
        raise RuntimeError(
            "Failed to init NCCL process group. Run via "
            "torchrun --nproc_per_node=<N> async_pipeline_optimizer.py"
        ) from exc

    rank = int(os.environ["LOCAL_RANK"])
    world_size = int(os.environ["WORLD_SIZE"])

    try:
        torch.cuda.set_device(rank)
        optimizer = AsyncPipelineOptimizer(rank=rank, world_size=world_size)

        # Warm-up: JIT/cudnn autotune, NCCL connection setup, cache warmup.
        for _ in range(3):
            optimizer.run_sync_pipeline(steps=2)
            optimizer.run_async_pipeline(steps=2)

        steps = 20
        sync_latency = optimizer.run_sync_pipeline(steps=steps)
        async_latency = optimizer.run_async_pipeline(steps=steps)

        if rank == 0:
            speedup_pct = (sync_latency - async_latency) / sync_latency * 100
            print("\n" + "=" * 50)
            print("  7-GPU PIPELINE PARALLEL ASYNC OVERLAP BENCHMARK")
            print("=" * 50)
            print(f"[-] Sync pipeline avg latency:   {sync_latency * 1000:.2f} ms")
            print(f"[+] Async pipeline avg latency:  {async_latency * 1000:.2f} ms")
            print(f"[*] Speedup:                     {speedup_pct:.2f}%")
            print("=" * 50 + "\n")
    finally:
        # Always tear down NCCL group even if benchmark raised.
        if dist.is_initialized():
            dist.destroy_process_group()


if __name__ == "__main__":
    main()
