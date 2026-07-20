"""Dure central control plane.

The API factory is lazy so importing core Agent modules never requires the optional
server dependency set.
"""


def create_app(*args, **kwargs):
    from .api import create_app as factory

    return factory(*args, **kwargs)


__all__ = ["create_app"]
