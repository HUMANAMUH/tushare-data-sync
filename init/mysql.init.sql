CREATE USER IF NOT EXISTS data IDENTIFIED BY 'showmethemoney';
CREATE USER IF NOT EXISTS data@localhost IDENTIFIED BY 'showmethemoney';
flush privileges;
GRANT ALL privileges ON tushare.* TO data@'%';
GRANT ALL privileges ON tushare.* TO data@'localhost';
flush privileges;

CREATE DATABASE IF NOT EXISTS tushare;
