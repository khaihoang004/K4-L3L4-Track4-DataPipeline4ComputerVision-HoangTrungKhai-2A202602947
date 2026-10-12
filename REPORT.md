# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Hoàng Trung Khải` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602947` |
| GitHub | `khaihoang004` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-00722` |

## Kết quả cuối

- `dataset_version` của bạn: `40c5e60cd5cc`, khớp `report/evidence/run_summary.json` sau lần chạy cuối.
- Số khung hình train / val / test của bạn: **694 / 149 / 176**, tổng **1.019 khung**, thuộc **18 / 4 / 4 trip**.
- `report/verify.txt`: đã lưu bằng `python3 scripts/check_submission.py --save-verify`, kết quả **27/27 checks passed**.
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Qua truy vấn hai bảng `lake.bronze_raw_telemetry_a` và `lake.bronze_raw_telemetry_b`, tôi thấy các khác biệt sau:

| Nội dung | Nguồn A | Nguồn B |
|---|---|---|
| Thời gian | `ts_utc`, chuỗi ISO UTC, ví dụ `2025-10-07T08:00:00.014Z` | `timestamp_ms`, epoch millisecond, ví dụ `1759933801509` |
| Tốc độ | `speed_mps`, đơn vị m/s | `spd_kmh`, đơn vị km/h |
| Bàn đạp | `gas`, `brake`, tỷ lệ 0–1 | `throttle_pct`, `brake_pct`, phần trăm 0–100 |
| Tên cột vị trí và hướng | `lat`, `lon`, `alt_m`, `heading_deg` | `latitude`, `longitude`, `altitude`, `hdg` |
| Tên cột gia tốc và góc lái | `acc_x`, `acc_y`, `steer` | `ax`, `ay`, `steering` |
| Độ lệch đồng hồ khai báo trong `bronze_video_files` | 0 ms | 1.500 ms; đồng hồ telemetry chạy trước đồng hồ video |

Pipeline giữ mỗi nguồn một bảng Bronze để lưu đúng schema gốc và truy vết riêng từng nguồn khi schema thay đổi. Các cột dữ liệu từ CSV được giữ ở dạng chữ, kể cả ô trống, để chưa làm mất thông tin hoặc khiến quá trình nạp thất bại do ép kiểu. Việc đổi đơn vị, chuyển kiểu thời gian, sửa lệch đồng hồ và phân loại dòng lỗi được thực hiện ở Silver; dữ liệu gốc vẫn còn để đối chiếu hoặc xử lý lại. Các cột metadata do pipeline thêm có thể có kiểu riêng, như `_row_no` là số nguyên và `_ingested_at` là timestamp UTC.

Kết quả Bronze của tôi có **3.021 dòng nguồn A**, **3.234 dòng nguồn B**, **26 trip** và **2 bản ghi video**; `scripts/lab.sh verify --module 1` đạt **5/5 checks passed**. Sau bước 1 có lượt nạp lại, lịch sử `bronze.raw_telemetry_a` vẫn chỉ có một commit `WRITE`, version **0**, thêm **3.021 dòng**. Kết quả này phù hợp với cơ chế nhận diện nội dung bằng SHA-256 và kiểm tra `_batch_id` trước khi ghi, giúp nạp lại cùng dữ liệu mà không nhân đôi.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Nguồn A giữ tốc độ m/s và chuyển timestamp ISO sang `timestamptz`. Với nguồn B, staging đổi tên cột về schema chung, chia `spd_kmh` cho 3,6, chia giá trị bàn đạp phần trăm cho 100, và chuyển epoch millisecond sang timestamp sau khi trừ **1.500 ms** độ lệch đồng hồ. Hai nguồn được hợp nhất rồi phân loại thành dữ liệu sạch hoặc dữ liệu cách ly.

Kết quả bước 2: A có **2.968 dòng sạch**, B có **3.181 dòng sạch**, tốc độ tối đa lần lượt **71,0 km/h** và **100,0 km/h**. Telemetry B bắt đầu tại **2025-10-08 14:30:00.009 UTC**, chỉ sau video bắt đầu lúc **14:30:00 UTC** khoảng **9 ms**, cho thấy độ lệch 1,5 giây đã được trừ. Kiểm tra module 2 đạt **7/7**.

| Drive | `duplicate` | `speed_missing` | `speed_out_of_range` | Tổng cách ly |
|---|---:|---:|---:|---:|
| `drive_a` | 20 | 30 | 3 | 53 |
| `drive_b` | 20 | 30 | 3 | 53 |

Các số khớp phép bảo toàn số dòng: A có **2.968 + 53 = 3.021**, B có **3.181 + 53 = 3.234**. Dòng lỗi nằm trong `silver.silver_telemetry_rejects` kèm lý do để có thể truy vết, thống kê chất lượng nguồn và xử lý lại khi có quy tắc phù hợp, thay vì mất dấu do bị xoá.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` có mức `warn` vì khoảng mất tín hiệu cảm biến là lỗi đã biết và được chấp nhận trong lab; pipeline vẫn ghi nhận cảnh báo và kiểm tra độ đồng bộ ở cấp khung hình. `in_range` dùng mức `error` vì tốc độ ngoài **0–70 m/s** không được phép đi vào dữ liệu sạch và dataset huấn luyện.

Trong `report/evidence/break_gate.json`, khi nới luật làm sạch lên **1.000 m/s**, test **`in_range_silver_telemetry_speed_mps__70__0`** bắt được **6 dòng** sai: kết quả **PASS=5 WARN=1 ERROR=1 SKIP=0** và `gate_failed=true`. Khi khôi phục luật gốc, kết quả trở lại **PASS=6 WARN=1 ERROR=0 SKIP=0**. Thí nghiệm cho thấy cổng chất lượng kiểm tra độc lập với luật biến đổi: dù bước làm sạch cho dữ liệu xấu lọt qua, test vẫn phát hiện và trả lỗi để chặn các bước phía sau.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm với **`trip-00003`** ghi trong `idempotency.json` cho thấy bảng `silver.frames` có **1.246 dòng** trước và sau hai lần chạy lại; dấu vân tay đều là **`6c7480dfde7e`**, và `unchanged=true`. Trip này có **60 khung**; ghi kiểu append hai lần vào bảng thử tạo **120 dòng**, tức gấp đôi số khung của trip.

Delta overwrite theo predicate `trip_id` thay thế dữ liệu của đúng trip trong một commit, giữ nguyên các trip khác. Người đọc thấy snapshot trước hoặc sau commit, tránh trạng thái partition đã bị xoá nhưng chưa ghi lại đủ như cách xoá rồi ghi qua hai thao tác riêng. Nếu ghi Delta thất bại trước commit, snapshot đã commit vẫn còn để chạy lại. Tính nguyên tử này áp dụng cho bảng Delta; ảnh JPG trên object storage được ghi riêng, không cùng một transaction với bảng.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Pipeline coi khung có độ sáng trung bình **< 12** là đen, còn độ sáng **từ 12 đến dưới 40** là tối. Khung đen bị loại khỏi Gold; khung tối chỉ được gắn cờ, vẫn có thể được giữ nếu đạt các điều kiện khác. Tôi đồng ý vì cảnh đường hầm hoặc bóng râm vẫn là dữ liệu cần học, trong khi khung gần như đen chứa rất ít thông tin. Bước 3 của tôi có **602 khung A**, **644 khung B**: khung đen **6/4**, khung mờ **9/0**, khung tối **4/61**, khung gần trùng **149/53**, mất đồng bộ **7/7**. Các cờ có thể chồng lên cùng một khung nên không cộng trực tiếp để tính số khung bị loại.

Theo `threshold.json`, ngưỡng 6 đánh dấu **363/602 khung A** và **244/644 khung B**, cao hơn nhiều so với ngưỡng 2. Tôi chọn **2** làm mức ban đầu để giảm khung lặp mà chưa loại quá mạnh các thay đổi nhỏ thật sự; việc chọn tốt nhất vẫn cần xem mẫu ảnh và đánh giá mô hình. Khi xe chậm hoặc đứng yên, hai khung liên tiếp tự nhiên giống nhau hơn, nên dHash nhỏ không đủ kết luận camera bị đứng hình; khi xe nhanh, ngưỡng quá cao có thể loại các khung có thông tin mới.

Kiểm tra module 3 đạt **6/6**, unit test đạt **9/9**. Độ trễ nối theo event time trung bình là **66 ms ở A**, **73 ms ở B**, nhưng tối đa **2.884/3.112 ms** do khoảng thiếu telemetry; Gold chỉ giữ khung đồng bộ trong ngưỡng **150 ms**.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trong `leakage.json`, **1.019 khung thuộc 26 trip** được dùng để so sánh. Chia theo khung hình làm **26/26 trip** nằm ở nhiều split và cả **26 trip** có khung trong cả train lẫn test; chia theo trip của pipeline làm **0 trip** bị chia cắt.

Các khung gần nhau trong cùng chuyến thường có cảnh, ánh sáng và vật thể tương tự. Khi chúng xuất hiện ở cả train và test, mô hình có thể dựa vào cảnh gần như đã thấy thay vì khái quát sang chuyến mới, khiến điểm test lạc quan hơn năng lực thật. Chia theo trip giữ toàn bộ một cảnh trong một split; để đánh giá triển khai xa hơn vẫn cần xem xét tách theo ngày, tuyến đường hoặc xe.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Trong mã lab, `dataset_version` là **12 ký tự đầu của SHA-256** tính trên các cặp **`frame_id|split` đã sắp xếp theo `frame_id`**. Cùng tập khung và cùng cách chia thì giữ nguyên version dù thời điểm export khác; ngày giờ hoặc số đếm sẽ tạo tên mới ngay cả khi nội dung không đổi. Đây là băm danh sách khung và split, không phải băm toàn bộ byte ảnh hoặc tất cả trường telemetry.

Hai lần export liên tiếp với MSSV **`2A202602947`** đều cho version **`40c5e60cd5cc`** và split **694 train / 149 val / 176 test**. MSSV được nối với `trip_id` để tính MD5 xếp thứ tự trip trong mỗi drive, nên MSSV khác có thể làm đổi các trip thuộc từng split và đổi version. Khi thử MSSV **`2A202602948`** qua biến môi trường tạm của một lệnh, version đổi thành **`b3bbff2858a5`**; sau thí nghiệm tôi chạy lại với MSSV thật. File `.env` vẫn giữ MSSV đầy đủ, gồm cả chữ.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest lưu tại `s3://lake/gold/datasets/40c5e60cd5cc/manifest.json`; bản chụp ở checkpoint Gold nằm trong `report/evidence/gold_manifest.json`. Dataset dùng hai bảng Bronze telemetry A/B version **0**, `bronze.trips_raw` version **0**, `bronze.video_files` version **1**, `silver.frames` version **27**, và các bảng `silver.telemetry`, `silver.frames_flagged`, `silver.frame_telemetry` version **0**. Hai video đầu vào có SHA-256:

- A: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`.
- B: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`.

Để tái tạo sau ba tháng, cần giữ dữ liệu gốc đúng hash, metadata trip/nguồn, MSSV **`2A202602947`**, commit mã và cấu hình xử lý, cùng manifest liệt kê khung/split và lịch sử Delta tương ứng. Version bảng là snapshot trong một lake cụ thể; reset lake sẽ tạo lịch sử mới, nên không được chỉ dựa vào số version để nhận diện dữ liệu. `dataset_version` riêng cũng chưa đủ xác nhận mọi byte ảnh hoặc nhãn telemetry không đổi.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Phần số MSSV cho **202602947 % 26 = 1**, tương ứng **`trip-00722`**, Map Index **1**. Trong lần chạy `lab_failure_recovery_20261013`, đúng **1 task `silver_frames` thất bại**, **7 task downstream ở trạng thái `upstream_failed`**, còn **29 task instance thành công**. Log lần thử đầu ghi `simulated crash while extracting trip-00722`; xem `airflow_failure.json` và ảnh `airflow_failure_log.png`.

Clear riêng task lỗi giúp trip này thành công ở **lần thử 2**, nhưng 7 task downstream vẫn bị chặn; `airflow_after_clear.json` ghi **30 success / 7 upstream_failed**. Vì vậy phải clear thêm đúng 7 task bị chặn, giữ nguyên các task đã thành công. Kết quả cuối trong `airflow_recovered.json` là **37/37 task instance success**: chỉ trip lỗi được trích xuất lại; 25 trip khác vẫn có một lần thử, và 7 task downstream chạy thành công ở lần thử đầu. Điểm này khác hướng dẫn giản lược rằng chỉ clear task đỏ là đủ.

Ở lần kiểm tra cuối, truy vấn `silver.frames` cho **1.246 dòng và 1.246 `frame_id` duy nhất**, nên bảng hiện tại không có khung bị nhân đôi; `report/verify.txt` đạt **27/27**. `run_summary.json` vẫn ghi version **`40c5e60cd5cc`**, khớp checkpoint Gold trước thí nghiệm, cùng split **694 / 149 / 176**. Số đếm hiện tại được kiểm tra sau lần chạy cuối từ lake trống; bằng chứng không nhân đôi khi chạy lại một partition nằm riêng trong `idempotency.json`. Ảnh `airflow_graph.png` ghi lại Graph xanh sau phục hồi.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi tăng lên 500 xe, lượng video nhiều hơn nhưng vẫn chỉ có một máy xử lý lần lượt, nên hàng đợi dài và pipeline có thể không hoàn thành kịp. Cách mở rộng là phân chia công việc cho nhiều máy xử lý.

1. **Xử lý video:** `silver_frames` chỉ chạy một trip cùng lúc trên LocalExecutor. Tôi sẽ tách worker xử lý video sang nhiều máy, dùng CeleryExecutor hoặc KubernetesExecutor, đồng thời giữ khả năng chạy lại riêng từng trip. 
2. **Biến đổi và join:** dbt dùng chung một file DuckDB trên máy lab; lượng dữ liệu lớn sẽ tăng nhu cầu RAM và thời gian xử lý. Tôi sẽ chuyển phần biến đổi sang Spark SQL trên các partition Delta và giữ lại các quality gate.
3. **Lưu trữ và I/O:** MinIO dùng một volume; mỗi trip tải video drive riêng, rồi ghi từng ảnh JPG và copy sang Gold. Tôi sẽ dùng object storage phân tán, cache video và gom ảnh thành shard khi phục vụ huấn luyện để giảm việc đọc lặp và số request nhỏ.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi thêm câu hỏi **“Trip nào có tỷ lệ khung bị loại cao nhất?”** vào dashboard `http://localhost:3000/dashboard/2`:

```sql
SELECT drive_id, trip_id, frames, frames_kept,
       frames - frames_kept AS frames_dropped,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS dropped_pct,
       avg_speed_kmh, split
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC, trip_id;
```

Kết quả trong `report/evidence/metabase_question.json`: **`trip-04622` của drive A bị loại 34/60 khung (56,67%)**, cao nhất; tiếp theo `trip-03904` là **11/22 (50%)**, `trip-02086` là **28/60 (46,67%)**. Câu hỏi giúp ưu tiên kiểm tra chất lượng từng trip; số khung bị loại tính bằng tổng trừ số giữ lại để không đếm trùng các cờ. Tỷ lệ này chưa chỉ ra nguyên nhân cụ thể, nên cần đối chiếu cờ ở Silver và mẫu ảnh trước khi kết luận.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điểm đáng chú ý là cùng một khung hình có thể mang nhiều cờ lỗi, nên cộng các cột lỗi lại chưa chắc ra số khung bị loại.
Tôi muốn tìm hiểu thêm cách chọn ngưỡng dHash để phân biệt camera đứng hình với cảnh ít thay đổi khi xe chạy chậm.
Pipeline kiểm tra tốc độ nhưng có thể vẫn bỏ sót GPS nhảy vị trí; có thể thêm kiểm tra tính nhất quán giữa vị trí, tốc độ và thời gian.
