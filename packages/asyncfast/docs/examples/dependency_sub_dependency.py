from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Depends
from asyncfast import Header

app = AsyncFast()


def get_tenant_id(tenant_id: Annotated[str, Header(alias="tenant-id")]) -> str:
    return tenant_id


def get_context(
    tenant_id: Annotated[str, Depends(get_tenant_id)],
) -> dict[str, str]:
    return {"tenant_id": tenant_id}


@app.channel("billing")
async def handle_billing(
    context: Annotated[dict[str, str], Depends(get_context)],
) -> None:
    print(context["tenant_id"])
