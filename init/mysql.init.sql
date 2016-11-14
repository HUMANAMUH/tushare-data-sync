DROP USER data;
CREATE USER data IDENTIFIED BY 'showmethemoney';
flush privileges;
GRANT ALL privileges ON tushare.* TO data@'%';
flush privileges;

CREATE DATABASE tushare;
