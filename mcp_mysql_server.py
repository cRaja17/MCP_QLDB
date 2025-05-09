import asyncio
import websockets
import json
import mysql.connector
import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Any
load_dotenv()


# Lấy schema từ MySQL
def get_db_schema() -> str:
    try:
        # connection = mysql.connector.connect(
        #     host=os.getenv('MYSQL_HOST'),
        #     user=os.getenv('MYSQL_USER'),
        #     password=os.getenv('MYSQL_PASS'),
        #     database=os.getenv('MYSQL_DB')
        # )
        # cursor = connection.cursor(dictionary=True)
        
        # cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", ("qldb",))
        # tables = [row["table_name"] for row in cursor.fetchall()]
        
        schema_description = """
            Table: duan ( Dự án )
            Columns:
            - DuAnID: INT (ID duy nhất của dự án)
            - TenDuAn: VARCHAR (Tên dự án)
            - TinhThanh: VARCHAR (Tỉnh/Thành phố thực hiện dự án)
            - ChuDauTu: VARCHAR (Chủ đầu tư dự án)
            - NgayKhoiCong: DATE (Ngày khởi công dự án)
            - TrangThai: VARCHAR (Trạng thái hiện tại của dự án)
            - NguonVon: VARCHAR (Nguồn vốn của dự án)
            - TongChieuDai: FLOAT (Tổng chiều dài tuyến thi công)
            - KeHoachHoanThanh: DATE (Ngày dự kiến hoàn thành)
            - MoTaChung: TEXT (Mô tả chung về dự án)
            - ParentID: INT (ID dự án cha (nếu là dự án thành phần))
            Foreign Keys:
            - ParentID references duan(DuAnID)

            Table: goithau ( Gói thầu)
            Columns:
            - GoiThau_ID: INT (ID duy nhất của gói thầu)
            - TenGoiThau: VARCHAR (Tên gói thầu)
            - DuAn_ID: INT (Khóa ngoại đến bảng dự án)
            - GiaTriHĐ: VARCHAR (Giá trị hợp đồng tính bằng (vnd))
            - Km_BatDau: VARCHAR (Km bắt đầu của gói thầu )
            - Km_KetThuc: VARCHAR (Km kết thúc của gói thầu )
            - ToaDo_BatDau_X: DECIMAL (Tọa độ X điểm bắt đầu)
            - ToaDo_BatDau_Y: DECIMAL (Tọa độ Y điểm bắt đầu)
            - ToaDo_KetThuc_X: DECIMAL (Tọa độ X điểm kết thúc)
            - ToaDo_KetThuc_Y: DECIMAL (Tọa độ Y điểm kết thúc)
            - NgayKhoiCong: DATE (Ngày khởi công gói thầu)
            - NgayHoanThanh: DATE (Ngày hoàn thành dự kiến)
            - TrangThai: VARCHAR (Trạng thái hiện tại của gói thầu)
            - NhaThauID: INT (Nhà thầu đảm nhận gói thầu)
            Foreign Keys:
            - NhaThauID references nhathau(NhaThauID)
            - DuAn_ID references duan(DuAnID)

            Table: goithau_nhathau (Gói thầu nhà thầu)
            Columns:
            - GoiThau_ID: INT (ID gói thầu)
            - NhaThauID: INT (id nhà thầu)
            - VaiTro: VARCHAR (vai trò)
            Foreign Keys:
            - GoiThau_ID references goithau(GoiThau_ID)
            - NhaThauID references nhathau(NhaThauID)

            Table: hangmuc (Hạng mục)
            Columns:
            - GoiThauID: INT (ID của gói thầu trong hạng mục)
            - HangMucID: INT ( ID của hạng mục)
            - TenHangMuc: VARCHAR (Tên hạng mục)
            - LoaiHangMuc: VARCHAR (Loại hạng mục)
            - TieuDeChiTiet: VARCHAR (Tiêu đề chi tiết)
            - MayMocThietBi: VARCHAR (Máy móc thiết bị)
            - NhanLucThiCong: VARCHAR (Nhân lực nhân công)
            - ThoiGianHoanThanh: DATE (Thời gian hoàn thành)
            - GhiChu: TEXT (Ghi chú)
            Foreign Keys:
            - GoiThauID references goithau(GoiThau_ID)

            Table: khoiluong_thicong (Khối lượng thi công)
            Columns:
            - KhoiLuong_ID: INT (ID của khối lượng thi công)
            - GoiThau_ID: INT (Tham chiếu đến bảng goithau)
            - NhaThauID: INT (Tham chiếu đến bảng nhathau)
            - TieuDe: VARCHAR (Tiêu đề)
            - NoiDung: TEXT (Nội dung)
            Foreign Keys:
            - GoiThau_ID references goithau(GoiThau_ID)
            - NhaThauID references nhathau(NhaThauID)

            Table: nhathau ( Nhà thầu)
            Columns:
            - NhaThauID: INT (ID nhà thầu)
            - TenNhaThau: VARCHAR (Tên nhà thầu)
            - Loai: VARCHAR (Loại)
            - MaSoThue: VARCHAR (Mã số thuế)
            - DiaChiTruSo: VARCHAR (Địa chỉ trụ sở)
            - SoDienThoai: VARCHAR (Số điện thoại)
            - Email: VARCHAR (Email)
            - NguoiDaiDien: VARCHAR (Người đại diện)
            - ChucVuNguoiDaiDien: VARCHAR (Chức vụ người đại diện)
            - GiayPhepKinhDoanh: VARCHAR (Giấy phép kinh doanh)
            - NgayCap: DATE (Ngày cấp)
            - NoiCap: VARCHAR (Nơi cấp)
            - GhiChu: TEXT (Ghi chú)

            Table: phanquyen (Phân quyền)
            Columns:
            - PhanQuyenID: INT ( )
            - TenQuyen: VARCHAR ( )
            - MoTaQuyen: TEXT ( )

            Table: quanlykehoach (Quản lý kế hoạch)
            Columns:
            - KeHoachID: INT (ID của kế hoạch)
            - HangMucID: INT (ID của hạng mục)
            - NhaThauID: INT (ID nhà thầu)
            - TenCongTac: VARCHAR (Tên công tác)
            - KhoiLuongKeHoach: FLOAT (Khối lượng kế hoạch)
            - DonViTinh: VARCHAR (Đơn vị tính)
            - NgayBatDau: DATE (Ngày bắt đầu)
            - NgayKetThuc: DATE (Ngày kết thúc)
            - GhiChu: VARCHAR (Ghi chú)
            Foreign Keys:
            - HangMucID references hangmuc(HangMucID)
            - NhaThauID references nhathau(NhaThauID)

            Table: taikhoan (Tài khoản)
            Columns:
            - NguoiDungID: INT ( )
            - TenDangNhap: VARCHAR ( )
            - MatKhau: VARCHAR ( )
            - HoTen: VARCHAR ( )
            - Email: VARCHAR ( )
            - SoDienThoai: VARCHAR ( )
            - ChucVu: VARCHAR ( )
            - DonViCongTac: VARCHAR ( )
            - PhanQuyenID: INT ( )
            - TrangThai: BIT ( )
            Foreign Keys:
            - PhanQuyenID references phanquyen(PhanQuyenID)

            Table: tiendothuchien (Tiến độ thực hiện)
            Columns:
            - TienDoID: INT (ID của tiến độ)
            - KeHoachID: INT (ID của kế hoạch)
            - NgayCapNhat: DATE (Ngày cập nhật)
            - KhoiLuongThucHien: FLOAT (Khối lượng thực hiện)
            - DonViTinh: VARCHAR (Đơn vị tính)
            - MoTaVuongMac: VARCHAR (Mô tả vướng mắc)
            - GhiChu: VARCHAR (Ghi chú)
            Foreign Keys:
            - KeHoachID references quanlykehoach(KeHoachID)

            
        """
        # for table in tables:
        #     cursor.execute("""
        #         SELECT column_name, data_type, column_comment
        #         FROM information_schema.columns 
        #         WHERE table_schema = %s AND table_name = %s
        #     """, ("qldb", table))
        #     columns = cursor.fetchall()
            
        #     schema_description += f"Table: {table}\nColumns:\n"
        #     for col in columns:
        #         comment = col['column_comment'] or " "
        #         schema_description += f"- {col['column_name']}: {col['data_type'].upper()} ({comment})\n"
            
        #     cursor.execute("""
        #         SELECT column_name, referenced_table_name, referenced_column_name
        #         FROM information_schema.key_column_usage
        #         WHERE table_schema = %s AND table_name = %s AND referenced_table_name IS NOT NULL
        #     """, ("qldb", table))
        #     foreign_keys = cursor.fetchall()
        #     if foreign_keys:
        #         schema_description += "Foreign Keys:\n"
        #         for fk in foreign_keys:
        #             schema_description += f"- {fk['column_name']} references {fk['referenced_table_name']}({fk['referenced_column_name']})\n"
        #     schema_description += "\n"
            
        # print(schema_description)
        # cursor.close()
        # connection.close()
        return schema_description
    except mysql.connector.Error as err:
        return f"Error retrieving schema: {str(err)}"


# Thực thi truy vấn MySQL
def query_mysql(sql_query: str) -> List[Dict[str, Any]]:
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASS'),
            database=os.getenv('MYSQL_DB')
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute(sql_query)
        results = cursor.fetchall()
        cursor.close()
        connection.close()
        return results
    except mysql.connector.Error as err:
        return [{"error": f"MySQL Error: {str(err)}"}]


# Phân tích câu hỏi để chọn bảng
def get_target_tables(question: str) -> List[str]:
    question = question.lower()
    tables = []
    
    if "dự án" in question or "du an" in question:
        tables.append("duan")
    if "nhà thầu" in question or "nha thau" in question:
        tables.append("nhathau")
    if "khối lượng" in question or "khoi luong" in question:
        tables.append("khoiluong")
    
    return tables if tables else ["duan"]


# Tạo truy vấn SQL với Ollama
def generate_sql_query(question: str) -> str:
    api_url = "http://localhost:11434/api/generate"
    table_schema = get_db_schema()
    target_tables = get_target_tables(question)
    target_tables_str = ", ".join(target_tables)
    print(target_tables_str)
    prompt = f"""
You are an expert in generating MySQL queries. Based on the following database schema and user question, generate a valid MySQL SELECT query for the appropriate table(s). Return only the SQL query, without any explanation or extra text.

{table_schema}

User question: {question}

Example:
- Question: "Tìm dự án ở Hà Nội"
  Query: SELECT DuAnID, TenDuAn, TinhThanh, LoaiDuAn FROM duan WHERE TinhThanh = 'Hà Nội'
- Question: "Tìm nhà thầu ở Hà Nội"
  Query: SELECT id, name, specialty, location FROM contractors WHERE location = 'Hà Nội'
- Question: "Tìm khối lượng của dự án Quốc lộ 1A"
  Query: SELECT k.id, k.TenKhoiLuong, k.SoLuong, d.TenDuAn 
         FROM khoiluong k 
         JOIN duan d ON k.DuAnID = d.DuAnID 
         WHERE d.TenDuAn = 'Dự án Quốc lộ 1A'

Return the SQL query:
"""

    payload = {
        "model": "llama3",
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.3,
            "num_predict": 200
        }
    }

    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()
        result = response.json()
        sql_query = result.get("response", "").strip()
        
        if sql_query.startswith("SELECT"):
            return sql_query
        else:
            return f"SELECT * FROM {target_tables[0]} LIMIT 10"
    except requests.RequestException as e:
        print(f"Error calling Ollama API: {e}")
        return f"SELECT * FROM {target_tables[0]} LIMIT 10"


# Tạo câu trả lời tự nhiên với Ollama
def generate_answer(question: str, results: List[Dict[str, Any]], target_tables: List[str]) -> str:
    api_url = "http://localhost:11434/api/generate"
    target_tables_str = ", ".join(target_tables)
    result_summary = json.dumps(results[:2], ensure_ascii=False)

    prompt = """
    You are an expert in generating natural language responses. Based on the user question, target tables, and query results, generate a concise, natural Vietnamese response summarizing the findings. Return only the response, without any explanation.

    Target tables: {}
    User question: {}
    Query results: {}

    Example:
    - Question: "Tìm dự án ở Hà Nội"
    Results: [{{"DuAnID": 1, "TenDuAn": "Dự án Quốc lộ 1A", "TinhThanh": "Hà Nội", "LoaiDuAn": "TONG"}}]
    Response: Tìm thấy 1 dự án tại Hà Nội
    - Question: "Tìm nhà thầu ở Hà Nội"
    Results: [{{"id": 1, "name": "Công ty ABC", "specialty": "Xây dựng", "location": "Hà Nội"}}]
    Response: Tìm thấy 1 nhà thầu tại Hà Nội chuyên xây dựng
    - Question: "Tìm khối lượng của dự án Quốc lộ 1A"
    Results: [{{"id": 1, "TenKhoiLuong": "Đào đất", "SoLuong": 1000, "TenDuAn": "Dự án Quốc lộ 1A"}}]
    Response: Tìm thấy 1 khối lượng cho dự án Quốc lộ 1A

    Response:
    """.format(target_tables_str, question, result_summary)

    payload = {
        "model": "llama3",
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.5,
            "num_predict": 50
        }
    }

    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()
        result = response.json()
        answer = result.get("response", "").strip()
        return answer if answer else f"Tìm thấy {len(results)} kết quả"
    except requests.RequestException as e:
        print(f"Error calling Ollama API for answer: {e}")
        return f"Tìm thấy {len(results)} kết quả"


# Xử lý yêu cầu JSON-RPC
async def handle_rpc_request(request: Dict) -> Dict:
    if request.get("jsonrpc") != "2.0" or "method" not in request or "id" not in request:
        return {
            "jsonrpc": "2.0",
            "error": {"code":-32600, "message": "Invalid Request"},
            "id": request.get("id", None)
        }

    method = request.get("method")
    params = request.get("params", {})

    if method != "query_projects":
        return {
            "jsonrpc": "2.0",
            "error": {"code":-32601, "message": f"Method {method} not found"},
            "id": request["id"]
        }

    question = params.get("question", "")
    if not question:
        return {
            "jsonrpc": "2.0",
            "error": {"code":-32602, "message": "Missing question parameter"},
            "id": request["id"]
        }

    # Tạo truy vấn SQL
    sql_query = generate_sql_query(question)
    
    # Thực thi truy vấn
    results = query_mysql(sql_query)

    # Kiểm tra lỗi
    if results and "error" in results[0]:
        return {
            "jsonrpc": "2.0",
            "error": {"code":-32000, "message": results[0]["error"]},
            "id": request["id"]
        }

    # Tạo câu trả lời
    target_tables = get_target_tables(question)
    print(target_tables)
    answer = generate_answer(question, results, target_tables)

    # Tạo phản hồi
    return {
        "jsonrpc": "2.0",
        "result": {
            "answer": answer,
            "data": results,
            "sql_query": sql_query
        },
        "id": request["id"]
    }


# WebSocket server
async def websocket_handler(websocket):
    try:
        async for message in websocket:
            try:
                request = json.loads(message)
                response = await handle_rpc_request(request)
                await websocket.send(json.dumps(response, ensure_ascii=False))
            except json.JSONDecodeError:
                response = {
                    "jsonrpc": "2.0",
                    "error": {"code":-32700, "message": "Parse error"},
                    "id": None
                }
                await websocket.send(json.dumps(response))
    except websockets.exceptions.ConnectionClosed:
        print("Client disconnected")


# Chạy server
async def main():
    server = await websockets.serve(websocket_handler, "localhost", 8765)
    print("MCP server running on ws://localhost:8765")
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
