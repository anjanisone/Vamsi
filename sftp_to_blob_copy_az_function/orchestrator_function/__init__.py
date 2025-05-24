import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    file_list = yield context.call_activity('list_files_from_network_share', None)
    batch_size = 1000
    for i in range(0, len(file_list), batch_size):
        batch = file_list[i:i + batch_size]
        yield context.call_activity('copy_network_files_batch', batch)
    return "All files copied."

main = df.Orchestrator.create(orchestrator_function)
