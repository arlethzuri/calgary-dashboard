import json


if __name__ == "__main__":
    # Example path to a JSON file (update as needed)
    example_path = "/Users/arleth/Desktop/calgary-dashboard/data/0_raw/enmax/Calgary_City_Limits/Calgary_City_Limits_features.json"
    
    try:
        data = load_json(example_path)
        print(f"Loaded JSON from: {example_path}")
        if isinstance(data, dict):
            print(f"Type: dict, Keys: {list(data.keys())}")
        elif isinstance(data, list):
            print(f"Type: list, Length: {len(data)}")
            if data and isinstance(data[0], dict):
                print(f"First item keys: {list(data[0].keys())}")
        else:
            print(f"Type: {type(data)}, Value: {data}")
    except FileNotFoundError:
        print(f"File not found: {example_path}")
    except Exception as e:
        print(f"Error loading JSON: {e}")