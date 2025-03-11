// custom_command.cpp
#include "glide/client.h"
#include "glide/config.h"


class CustomCommand {
    public:
#if HSET
    static bool execute(glide::Client& client) {
        // Your custom logic here
        std::map<std::string, std::string> field_values = {
            {"field1", "value1"},
            {"field2", "value2"}
        };
        return client.hset("custom_key", field_values);
    }
#elif MSET

    public:
    static bool execute(glide::Client& client) {
        std::map<std::string, std::string> key_value_map;
    for (int i = 0; i < 50; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        key_value_map[key] = value;
    }

    
    std::string response = client.mset(key_value_map);
    return true;
    }
#else
    public:
    static bool execute(glide::Client& client) {
        std::vector<std::string> key_value_map;
    for (int i = 0; i < 50; ++i) {
        std::string key = "key" + std::to_string(i);        
        key_value_map.push_back(key);
    }

    
    std::vector<std::string> response = client.mget(key_value_map);
    return true;
    }
#endif
};
