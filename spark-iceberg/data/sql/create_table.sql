create table rison_iceberg_db.ods_base_ad (
  adid string,
  adname string,
  dn string
) using iceberg
partitioned by (dn);

create table rison_iceberg_db.ods_base_website(
createtime string,
creator string,
delete string,
dn string,
siteid string,
sitename string,
siteurl string
) using iceberg
partitioned by (dn);

create table rison_iceberg_db.ods_member(
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
    dt            string,
    dn            string
) using iceberg
   partitioned by(dt);

create table rison_iceberg_db.ods_member_reg_type(
    uid           string,
    appkey        string,
    appregurl     string,
    bdp_uuid      string,
    createtime    string,
    isranreg      string,
    regsource     string,
    websiteid     string,
    dt            string,
    dn            String,
    domain        string
) using iceberg
  partitioned by(dt);

create table rison_iceberg_db.ods_vip_level(
    vip_id           string,
    vip_level        string,
    start_time       string,
    end_time         string,
    last_modify_time string,
    max_free         string,
    min_free         string,
    next_level       string,
    operator         string,
    dn               string,
    discountval      string
) using iceberg
 partitioned by(dn);

create table ods_p_center_mem_pay_money(
    uid      string,
    paymoney string,
    siteid   string,
    vip_id   string,
    dt       string,
    dn       string
) using iceberg
 partitioned by(dt,dn);

create table rison_iceberg_db.dwd_member
(
    uid           int,
    ad_id         int,
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
    dt            string
) using iceberg
   partitioned by(dt);


create table rison_iceberg_db.dwd_member_reg_type
(
    uid           int,
    appkey        string,
    appregurl     string,
    bdp_uuid      string,
    createtime    timestamp,
    isranreg      string,
    regsource     string,
    regsourcename string,
    websiteid     int,
    dt            string
) using iceberg
  partitioned by(dt);


create table rison_iceberg_db.dwd_base_ad
(
    adid   int,
    adname string,
    dn     string
) using iceberg
partitioned by (dn);


create table rison_iceberg_db.dwd_base_website
(
    siteid     int,
    sitename   string,
    siteurl    string,
    `delete`   int,
    createtime timestamp,
    creator    string,
    dn         string
) using iceberg
partitioned by (dn);

create table rison_iceberg_db.dwd_p_center_mem_pay_money
(
    uid      int,
    paymoney string,
    siteid   int,
    vip_id   int,
    dt       string,
    dn       string
) using iceberg   
 partitioned by(dt,dn);

create table rison_iceberg_db.dwd_vip_level
(
    vip_id           int,
    vip_level        string,
    start_time       timestamp,
    end_time         timestamp,
    last_modify_time timestamp,
    max_free         string,
    min_free         string,
    next_level       string,
    operator         string,
    dn               string
) using iceberg
 partitioned by(dn);


create table rison_iceberg_db.dws_member
(
    uid                  int,
    ad_id                int,
    fullname             string,
    iconurl              string,
    lastlogin            string,
    mailaddr             string,
    memberlevel          string,
    password             string,
    paymoney             string,
    phone                string,
    qq                   string,
    register             string,
    regupdatetime        string,
    unitname             string,
    userip               string,
    zipcode              string,
    appkey               string,
    appregurl            string,
    bdp_uuid             string,
    reg_createtime       timestamp,
    isranreg             string,
    regsource            string,
    regsourcename        string,
    adname               string,
    siteid               int,
    sitename             string,
    siteurl              string,
    site_delete          string,
    site_createtime      string,
    site_creator         string,
    vip_id               int,
    vip_level            string,
    vip_start_time       timestamp,
    vip_end_time         timestamp,
    vip_last_modify_time timestamp,
    vip_max_free         string,
    vip_min_free         string,
    vip_next_level       string,
    vip_operator         string,
    dt                   string,
    dn                   string
) using iceberg
partitioned by(dt,dn);


create table rison_iceberg_db.ads_register_appregurlnum
(
    appregurl string,
    num       int,
    dt        string,
    dn        string
) using iceberg
partitioned by(dt);

create table rison_iceberg_db.ads_register_top3memberpay
(
    uid           int,
    memberlevel   string,
    register      string,
    appregurl     string,
    regsourcename string,
    adname        string,
    sitename      string,
    vip_level     string,
    paymoney      decimal(10, 4),
    rownum        int,
    dt            string,
    dn            string
) using iceberg
partitioned by(dt);