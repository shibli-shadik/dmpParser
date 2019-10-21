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
 `created_at` varchar(50) NOT NULL,
 `tran_type` varchar(5) NOT NULL,
 `amount` varchar(50) NOT NULL,
 `case_id` varchar(100) NOT NULL,
 `status` varchar(5) NOT NULL,
 `rrn_no` varchar(50) NOT NULL   
)

CREATE TABLE `transactions` (
 `id` bigint(20) NOT NULL AUTO_INCREMENT,
 `created_at` datetime NOT NULL,
 `amount` varchar(50) NOT NULL,
 `case_id` varchar(50) NOT NULL,
 `status` varchar(200) NOT NULL,
 `rrn_no` varchar(50) NOT NULL,
 PRIMARY KEY (`id`)
) 