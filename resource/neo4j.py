def do(payload, config, plugin_config, inputs):
    print("HELLO FROM BACKEND")
    print(config)
    return {'nodes': ['a', 'b', 'c']}