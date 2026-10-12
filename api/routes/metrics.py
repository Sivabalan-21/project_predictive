from fastapi import APIRouter, Request, Response

router = APIRouter(tags=["metrics"])


@router.get("/metrics", include_in_schema=False)
def metrics(request: Request) -> Response:
    """Prometheus exposition. Public like /health: it contains only counters/histograms labelled with route templates -
    no user data, ids or secrets. Keep the port off untrusted networks (see docs/observability.md)."""
    body, content_type = request.app.state.metrics.render()
    return Response(content=body, media_type=content_type)
