package util

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strconv"
)

type Config struct {
	data map[string]interface{}
}

func newConfig() *Config {
	result := new(Config)
	result.data = make(map[string]interface{})
	return result
}

// Loads config information from a JSON file
func LoadConfigFile(filename string) *Config {
	result := newConfig()
	err := result.parse(filename)
	if err != nil {
		log.Fatalf("error loading config file %s: %s", filename, err)
	}

	return result
}

// Loads config information from a JSON string
func LoadConfigString(s string) *Config {
	result := newConfig()
	err := json.Unmarshal([]byte(s), &result.data)
	if err != nil {
		log.Fatalf("error parsing config string %s: %s", s, err)
	}

	return result
}

func (c *Config) parse(fileName string) error {
	jsonFileBytes, err := ioutil.ReadFile(fileName)
	if err == nil {
		err = json.Unmarshal(jsonFileBytes, &c.data)
	}

	return err
}

// Returns a string for the config variable key
func (c *Config) GetString(key string) string {
	result, present := c.data[key]
	if !present {
		return ""
	}

	return result.(string)
}

// Returns a int for the config variable key
func (c *Config) GetInt(key string) int {
	if x, ok := c.data[key]; ok {
		str := x.(string)
		if v, err := strconv.Atoi(str); err == nil {
			return v
		}
	}

	return -1
}

// Returns a float for the config variable key
func (c *Config) GetFloat(key string) float64 {
	x, ok := c.data[key]
	if !ok {
		return -1
	}

	return x.(float64)
}

// Returns a bool for the config variable key
func (c *Config) GetBool(key string) bool {
	x, ok := c.data[key]
	if !ok {
		return false
	}

	return x.(bool)
}

// Returns an interface for the config variable key
func (c *Config) GetInterface(key string) interface{} {
	result, present := c.data[key]
	if !present {
		return interface{}(nil)
	}

	return result.(interface{})
}

// Returns an array for the config variable key
func (c *Config) GetArray(key string) []interface{} {
	result, present := c.data[key]
	if !present {
		return []interface{}(nil)
	}

	return result.([]interface{})
}
