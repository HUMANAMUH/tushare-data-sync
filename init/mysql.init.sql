CREATE USER IF NOT EXISTS data IDENTIFIED BY 'showmethemoney';
CREATE USER IF NOT EXISTS data@localhost IDENTIFIED BY 'showmethemoney';
flush privileges;
GRANT ALL privileges ON tushare_buffer.* TO data@'%';
GRANT ALL privileges ON tushare_buffer.* TO data@'localhost';
GRANT ALL privileges ON tushare_tmp.* TO data@'%';
GRANT ALL privileges ON tushare_tmp.* TO data@'localhost';
GRANT ALL privileges ON tushare_data.* TO data@'%';
GRANT ALL privileges ON tushare_data.* TO data@'localhost';
flush privileges;

CREATE DATABASE IF NOT EXISTS tushare_buffer;
CREATE DATABASE IF NOT EXISTS tushare_tmp;
CREATE DATABASE IF NOT EXISTS tushare_data;
