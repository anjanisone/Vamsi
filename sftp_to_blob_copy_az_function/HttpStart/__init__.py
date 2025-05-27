import azure.functions as func
import azure.durable_functions as df

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    payload = await req.get_json()
    instance_id = await client.start_new("orchestrator_function", None, payload)
    return client.create_check_status_response(req, instance_id)
