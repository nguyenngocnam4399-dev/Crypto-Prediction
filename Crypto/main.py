import asyncio
import json
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

clients: set[WebSocket] = set()


async def binance_listener():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/bnbusdt@ticker"

    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as b_ws:
                while True:
                    data = await b_ws.recv()
                    msg = json.loads(data)

                    payload = json.dumps({
                        "symbol": msg["s"].lower(),
                        "close": float(msg["c"]),
                        "percent": float(msg["P"])
                    })

                    dead_clients = set()

                    for client in clients:
                        try:
                            await client.send_text(payload)
                        except Exception:
                            dead_clients.add(client)

                    # Xóa client chết
                    for dc in dead_clients:
                        clients.remove(dc)

        except Exception as e:
            print("Binance WS error:", e)
            await asyncio.sleep(3)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(binance_listener())


@app.websocket("/ws/price")
async def websocket_endpoint(websocket: WebSocket):

    await websocket.accept()
    clients.add(websocket)

    try:
        while True:
            # Chờ ping từ client, tránh timeout
            await websocket.receive_text()

    except WebSocketDisconnect:

        if websocket in clients:
            clients.remove(websocket)

    except Exception as e:

        print("Client error:", e)

        if websocket in clients:
            clients.remove(websocket)


# import asyncio
# import json
# import websockets
# from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# app = FastAPI()
# clients = set()

# async def binance_listener():
#     # Kết nối lấy Ticker của BTC, ETH, BNB
#     uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/bnbusdt@ticker"
#     async with websockets.connect(uri) as b_ws:
#         while True:
#             try:
#                 data = await b_ws.recv()
#                 msg = json.loads(data)
#                 # Đóng gói dữ liệu chuẩn: Giá hiện tại (c) và % thay đổi (P)
#                 payload = json.dumps({
#                     "symbol": msg['s'].lower(),
#                     "close": float(msg['c']),
#                     "percent": float(msg['P']) # Binance trả về % chuẩn 24h
#                 })
#                 if clients:
#                     await asyncio.gather(*[client.send_text(payload) for client in clients])
#             except Exception as e:
#                 print(f"Error: {e}")
#                 await asyncio.sleep(1)

# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(binance_listener())

# @app.websocket("/ws/price")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     clients.add(websocket)
#     try:
#         while True:
#             await websocket.receive_text()
#     except WebSocketDisconnect:
#         clients.remove(websocket)

# from fastapi import FastAPI, WebSocket, WebSocketDisconnect
# from fastapi.middleware.cors import CORSMiddleware
# import asyncio
# import json
# import websockets

# app = FastAPI()

# # Mở toàn bộ cổng để máy khách (máy bạn) truy cập được
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# clients = set()

# @app.websocket("/ws/price")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     clients.add(websocket)
#     print(f"Máy khách mới đã kết nối: {websocket.client}")
#     try:
#         while True:
#             await websocket.receive_text() # Giữ kết nối sống
#     except WebSocketDisconnect:
#         clients.remove(websocket)
#         print("Máy khách đã ngắt kết nối")

# # Tại Backend (Server 172.30.2.223)
# async def binance_listener():
#     uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/bnbusdt@ticker"
#     async with websockets.connect(uri) as b_ws:
#         while True:
#             data = await b_ws.recv()
#             msg = json.loads(data)
#             payload = json.dumps({
#                 "symbol": msg['s'].lower(),
#                 "close": float(msg['c']),    # Giá hiện tại
#                 "open": float(msg['p'])      # Giá biến đổi trong 24h (hoặc msg['o'])
#             })
#             if clients:
#                 await asyncio.gather(*[client.send_text(payload) for client in clients])

# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(binance_listener())

# # LỆNH CHẠY QUAN TRỌNG: uvicorn main:app --host 0.0.0.0 --port 8000
# # import asyncio
# # import json
# # import websockets
# # from fastapi import FastAPI, WebSocket, WebSocketDisconnect
# # from fastapi.middleware.cors import CORSMiddleware

# # app = FastAPI()

# # # CẤU HÌNH QUAN TRỌNG: Cho phép mọi máy trong mạng truy cập
# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=["*"],
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# # clients = set()

# # @app.websocket("/ws/price")
# # async def websocket_endpoint(websocket: WebSocket):
# #     await websocket.accept()
# #     clients.add(websocket)
# #     try:
# #         while True:
# #             await websocket.receive_text()
# #     except WebSocketDisconnect:
# #         clients.remove(websocket)

# # async def binance_listener():
# #     # Kết nối tới Binance để lấy giá thực
# #     uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/bnbusdt@ticker"
# #     async with websockets.connect(uri) as websocket:
# #         while True:
# #             data = await websocket.recv()
# #             msg = json.loads(data)
# #             # Chuẩn hóa dữ liệu gửi đi
# #             payload = json.dumps({
# #                 "symbol": msg['s'],      # Ví dụ: BTCUSDT
# #                 "close": float(msg['c']) # Giá hiện tại
# #             })
# #             if clients:
# #                 await asyncio.gather(*[client.send_text(payload) for client in clients])

# # @app.on_event("startup")
# # async def startup_event():
# #     asyncio.create_task(binance_listener())

# # # LƯU Ý: Khi chạy, hãy dùng lệnh: 
# # # uvicorn main:app --host 0.0.0.0 --port 8000