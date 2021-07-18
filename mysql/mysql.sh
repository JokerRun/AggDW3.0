###### 安装MySQL
# 1）安装mysql依赖
rpm -ivh 01_mysql-community-common-5.7.16-1.el7.x86_64.rpm
rpm -ivh 02_mysql-community-libs-5.7.16-1.el7.x86_64.rpm
rpm -ivh 03_mysql-community-libs-compat-5.7.16-1.el7.x86_64.rpm
# 2）安装mysql-client
rpm -ivh 04_mysql-community-client-5.7.16-1.el7.x86_64.rpm
# 3）安装mysql-server
rpm -ivh 05_mysql-community-server-5.7.16-1.el7.x86_64.rpm
# 4）启动mysql
systemctl start mysqld
# 5）查看mysql密码


##### 配置MySQL
# 配置只要是root用户+密码，在任何主机上都能登录MySQL数据库。
# 1）用刚刚查到的密码进入mysql（如果报错，给密码加单引号）
mysql -uroot -p 'password'
# 2）设置复杂密码(由于mysql密码策略，此密码必须足够复杂)
set password=password("Qs23=zs32");
# 3）更改mysql密码策略
set global validate_password_length=4;
set global validate_password_policy=0;
# 4）设置简单好记的密码
set password=password("000000");
# 5）进入msyql库
use mysql
# 6）查询user表
select user, host from user;
# 7）修改user表，把Host表内容修改为%
update user set host="%" where user="root";
# 8）刷新
flush privileges;
# 9）退出
quit;
