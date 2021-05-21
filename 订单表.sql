CREATE TABLE `trade` (
	`id` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
	`trade_no` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
	UNIQUE INDEX `id` (`id`),
	INDEX `trade_no` (`trade_no`)
)
COMMENT='订单'
COLLATE='utf8_unicode_ci'
ENGINE=InnoDB
;
