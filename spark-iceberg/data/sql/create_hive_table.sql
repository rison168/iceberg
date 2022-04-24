create table rison_hive_db.ods_base_ad (
  adid string,
  adname string
)
partitioned by (dn string)
row format delimited fields terminated by '\t';

create table rison_hive_db.ods_base_website(
createtime string,
creator string,
delete string,
siteid string,
sitename string,
siteurl string
)
partitioned by (dn string)
row format delimited fields terminated by '\t';

create table rison_hive_db.ods_member(
    uid           string,
    ad_id         string,
    birthday      string,
    email         string,
    fullname      string,
    iconurl       string,
    lastlogin     string,
    mailaddr      string,
    memberlevel   string,
    password      string,
    paymoney      string,
    phone         string,
    qq            string,
    register      string,
    regupdatetime string,
    unitname      string,
    userip        string,
    zipcode       string,
    dn            string
)
   partitioned by(dt string)
   row format delimited fields terminated by '\t';

create table rison_hive_db.ods_member_reg_type(
    uid           string,
    appkey        string,
    appregurl     string,
    bdp_uuid      string,
    createtime    string,
    isranreg      string,
    regsource     string,
    websiteid     string,
    dn            String,
    domain        string
)
  partitioned by(dt string)
  row format delimited fields terminated by '\t';

create table rison_hive_db.ods_vip_level(
    vip_id           string,
    vip_level        string,
    start_time       string,
    end_time         string,
    last_modify_time string,
    max_free         string,
    min_free         string,
    next_level       string,
    operator         string,
    discountval      string
)
 partitioned by(dn string)
 row format delimited fields terminated by '\t';

 create table rison_hive_db.ods_p_center_mem_pay_money(
    uid      string,
    paymoney string,
    siteid   string,
    vip_id   string
)
 partitioned by(dt string,dn string)
 row format delimited fields terminated by '\t';

