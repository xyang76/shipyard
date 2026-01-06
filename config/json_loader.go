package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

func LoadJson(filepath string) (Config, error) {
	var config Config

	// Read the JSON file
	file, err := ioutil.ReadFile(filepath)
	if err != nil {
		return config, fmt.Errorf("error reading config file: %v", err)
	}

	// Parse the JSON into the Config struct
	if err := json.Unmarshal(file, &config); err != nil {
		return config, fmt.Errorf("error parsing config file: %v", err)
	}

	return config, nil
}

func LoadShards(filepath string) (ShardConfig, error) {
	var config ShardConfig

	// Read the JSON file
	file, err := ioutil.ReadFile(filepath)
	if err != nil {
		return config, fmt.Errorf("error reading config file: %v", err)
	}

	// Parse the JSON into the Config struct
	if err := json.Unmarshal(file, &config); err != nil {
		return config, fmt.Errorf("error parsing config file: %v", err)
	}

	return config, nil
}

func WriteJson(filepath string, cfg Config) {
	// Convert the config structure to JSON
	jsonData, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	// Append the JSON data to a file
	err = ioutil.WriteFile(filepath, jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing file:", err)
		return
	}
}
