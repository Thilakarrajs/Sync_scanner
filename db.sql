CREATE TABLE `scanner_results` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `process_uuid` varchar(45) NOT NULL,
  `scan_core` varchar(45) DEFAULT NULL,
  `issue_type` varchar(245) DEFAULT NULL,
  `primary_key_value` varchar(545) DEFAULT NULL,
  `primary_data_schema` varchar(245) DEFAULT NULL,
  `primary_data_value` varchar(545) DEFAULT NULL,
  `secondary_data_schema` varchar(245) DEFAULT NULL,
  `secondary_data_value` varchar(545) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
);




CREATE TABLE `scanner_core` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `core_name` varchar(245) NOT NULL,
  `mapping_location` varchar(245) DEFAULT NULL,
  `primary_db_query` varchar(545) DEFAULT NULL,
  `secondary_db_query` varchar(545) DEFAULT NULL,
  `primary_db_primary_key` varchar(245) DEFAULT NULL,
  `secondary_db_primary_key` varchar(245) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
);


CREATE TABLE `core_running_status` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `core_name` varchar(545) DEFAULT NULL,
  `core_process_id` varchar(45) DEFAULT NULL,
  `processed_line` int(20) DEFAULT NULL,
  `last_offset` int(20) DEFAULT NULL,
  `is_complete` tinyint(1) DEFAULT '0',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
);


INSERT INTO scanner_core ( core_name, mapping_location, primary_db_query, secondary_db_query, primary_db_primary_key, secondary_db_primary_key) VALUES ( 'shippingOrder', '/home/thilakar/Sync_scanner/config/orders_mapping.csv', 'select * from shipping_order order by updated_at desc limit 50', 'select * from orders order by updated_at desc limit 50', 'order_id', 'order_id');