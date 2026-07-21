from __future__ import annotations

import click

from saem import agent as agent_mod
from saem import head as head_mod
from saem.common.state import ROLE_CHOICES


@click.group()
def main():
    """saem: install-once, role-assign-from-head cluster CLI."""


@main.command()
def agent():
    """Run this node's always-on agent (receives role assignments)."""
    agent_mod.run()


@main.group()
def head():
    """Commands run only on the designated head node."""


@head.command("start")
@click.option("--ip", required=True, help="this node's own internal IP (e.g. 192.168.0.228)")
def head_start(ip: str):
    """Mark this node as head."""
    head_mod.start(ip)
    click.echo(f"this node is now head ({ip})")


@head.command("register")
@click.argument("ip")
@click.option("--role", type=click.Choice(ROLE_CHOICES), required=True)
@click.option("--port", type=int, default=None)
def head_register(ip: str, role: str, port: int | None):
    """Assign ROLE to the node at IP (must be running `saem agent`)."""
    if not head_mod.is_head():
        raise click.ClickException("this node is not head; run `saem head start` first")
    result = head_mod.register(ip, role, port)
    click.echo(f"{ip} -> {result}")


@head.command("unregister")
@click.argument("ip")
@click.option(
    "--force", is_flag=True, help="drop from the registry even if the node is unreachable"
)
def head_unregister(ip: str, force: bool):
    """Stop the role on the node at IP and drop it from the registry."""
    if not head_mod.is_head():
        raise click.ClickException("this node is not head; run `saem head start` first")
    try:
        result = head_mod.unregister(ip, force=force)
    except RuntimeError as e:
        raise click.ClickException(str(e))
    click.echo(result)


@head.command("status")
def head_status():
    """List every node this head has registered."""
    for entry in head_mod.status():
        click.echo(entry)


@head.command("register-backend")
@click.argument("name")
@click.argument("url")
@click.option("--model", required=True, help="served model name, e.g. qwen3-235b")
@click.option(
    "--active/--no-active",
    default=True,
    help="push this backend to every registered retrieval_gateway/api_proxy node now",
)
def head_register_backend(name: str, url: str, model: str, active: bool):
    """Register a dure GPU-cluster head (e.g. camp1) as an LLM backend.

    Example: saem head register-backend cluster235b http://192.168.0.228:8000 --model qwen3-235b
    """
    if not head_mod.is_head():
        raise click.ClickException("this node is not head; run `saem head start` first")
    result = head_mod.register_backend(name, url, model, active=active)
    click.echo(result)


@head.command("unregister-backend")
@click.argument("name")
def head_unregister_backend(name: str):
    """Forget a backend; if it was active, consumer nodes are cleared too."""
    if not head_mod.is_head():
        raise click.ClickException("this node is not head; run `saem head start` first")
    try:
        result = head_mod.unregister_backend(name)
    except RuntimeError as e:
        raise click.ClickException(str(e))
    click.echo(result)


@head.command("backend-status")
def head_backend_status():
    """List every backend this head knows about."""
    for entry in head_mod.backend_status():
        click.echo(entry)
