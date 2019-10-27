CREATE TABLE `pre_staging` (
 `pos_id` varchar(50) NOT NULL,
 `terminal_id` varchar(50) NOT NULL,
 `tran_type` varchar(5) NOT NULL,
 `line_no` int NOT NULL,
 `data` varchar(300) NOT NULL
)

CREATE TABLE `staging` (
 `pos_id` varchar(50) NOT NULL,
 `terminal_id` varchar(50) NOT NULL,
 `payment_date` varchar(50) NOT NULL,
 `tran_type` varchar(5) NOT NULL,
 `amount` varchar(50) NOT NULL,
 `case_id` varchar(100) NOT NULL,
 `status` varchar(5) NOT NULL,
 `rrn_no` varchar(50) NOT NULL,
 `file_id` bigint(20) NOT NULL,
 FOREIGN KEY (file_id) REFERENCES file_register(id)   
)

CREATE TABLE `transactions` (
 `id` bigint(20) NOT NULL AUTO_INCREMENT,
 `created_at` datetime NOT NULL,
 `amount` varchar(50) NOT NULL,
 `case_id` varchar(50) NOT NULL,
 `rrn_no` varchar(50) NOT NULL,
 `pos_id` varchar(50) NOT NULL,
 `terminal_Id` varchar(50) NOT NULL,
 `message_code` varchar(20) NOT NULL,
 `message_response` varchar(500) NOT NULL,
 `tran_type` varchar(5) NOT NULL,
 `file_id` bigint(20) NOT NULL,
 PRIMARY KEY (`id`),
 FOREIGN KEY (file_id) REFERENCES file_register(id)
) 