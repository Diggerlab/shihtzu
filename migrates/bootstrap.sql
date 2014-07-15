create database shihtzu DEFAULT CHARACTER SET UTF8 COLLATE utf8_general_ci;
CREATE USER 'shihtzu'@'localhost' IDENTIFIED BY 'shihtzu';
grant all on shihtzu.* to 'shihtzu'@'localhost';