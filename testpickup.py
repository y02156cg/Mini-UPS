import requests
import json
import uuid
import time

url = "http://vcm-46875.vm.duke.edu:8000/api/amazon/"  # API

# payload = {
#     "action": "request_pickup",
#     "message_id": "test-msg-001",
#     "package_id": "PKG789",
#     "warehouse_id": 1,
#     "user_id": 1,
#     "destination_x": 25,
#     "destination_y": 30,
#     "description": "Test Package Delivery",
#     "items": [
#         {"name": "laptop", "description": "15-inch MacBook Pro", "quantity": 1},
#         {"name": "charger", "description": "USB-C 60W Adapter", "quantity": 2}
#     ]
# }

# headers = {"Content-Type": "application/json"}
# response = requests.post(url, headers=headers, data=json.dumps(payload))

# print("Status:", response.status_code)
# print("Response:", response.json())


# 第一步：发送 request_pickup
pickup_data = {
    "action": "request_pickup",
    "message_id": str(uuid.uuid4()),
    "warehouse_id": 1,
    "user_id": 1,
    "destination_x": 20,
    "destination_y": 25,
    "description": "Test delivery from pickup",
    "items": [
        {"name": "book", "description": "Django for APIs", "quantity": 1}
    ]
}

print(">>> Sending request_pickup...")
r1 = requests.post(url, json=pickup_data)
print("Status:", r1.status_code)
print("Response:", r1.json())
package_id = r1.json().get("tracking_number")
if not package_id:
    print("❌ Failed to get package_id from response.")
    exit(1)

# 可选：稍微等一下（模拟打包）
time.sleep(1)

# 第二步：发送 package_ready
ready_data = {
    "action": "package_ready",
    "message_id": str(uuid.uuid4()),
    "package_id": package_id
}

print("\n>>> Sending package_ready...")
r2 = requests.post(url, json=ready_data)
print("Status:", r2.status_code)
print("Response:", r2.json())