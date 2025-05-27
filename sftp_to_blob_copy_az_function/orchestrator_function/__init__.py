import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    file_list = context.get_input()
    batch_size = 1000
    for i in range(0, len(file_list), batch_size):
        batch = file_list[i:i + batch_size]
        yield context.call_activity('copy_files_from_payload', batch)
    return "All specified files copied."

main = df.Orchestrator.create(orchestrator_function)
