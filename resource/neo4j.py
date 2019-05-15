def do(payload, config, plugin_config, inputs):
    print("HELLO FROM BACKEND")
    return {'nodes': ['a', 'b', 'c']}