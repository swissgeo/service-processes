from app.core.authorization import Action, Resource


class DummyAuthorizer:
    """Authorizer allowing everything, for local development only."""

    async def is_authorized(self, token: str, action: Action, resource: Resource) -> bool:  # noqa: ARG002
        return True
