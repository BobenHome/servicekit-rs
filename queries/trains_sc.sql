-- name: get_train_sc_list
-- 获取四川培训班列表
SELECT a.ID                              AS _id,
       a.ID                              AS id,
       a.DATASTATE                       AS operation,
       a.TRAINID                         AS training_id,
       a.TRAINNAME                       AS training_name,
       tl.`NAME`                         AS train_level,
       tm.`NAME`                         AS train_mode,
       tc.`NAME`                         AS train_category,
       IF(CHAR_LENGTH(a.TRAINCONTENT) > 98, CONCAT(SUBSTRING(a.TRAINCONTENT, 1, 98), '...'),
          a.TRAINCONTENT)                AS train_content,
       a.TRAINPURPOSE                    AS train_purpose,
       a.TRAINOBJECT                     AS train_object,
       a.TRAINCLAIM                      AS train_claim,
       a.UNDERTAKEORGNAME                AS train_organizer,
       a.MSSCODE                         AS plan_id,
       tt.`NAME`                         AS train_type,
       CONCAT_WS(
               '/',
               '反应层',
               IF(locate('1', a.TRAINASSESSTYPE) > 0, '学习层', ''),
               IF(locate('2', a.TRAINASSESSTYPE) > 0, '行为层', ''),
               IF(locate('3', a.TRAINASSESSTYPE) > 0, '专项评估', '')
       )                                 AS train_assess_type,
       a.TRAINNINGDURATION               AS train_time,
       a.TRAINPEOPLENUMBER               AS train_people_number,
       a.TRAINSPONSORNUMBER              AS train_sponsor_number,
       a.TRAINUSERASSESS                 AS train_user_assess,
       a.TRAINSPONSORASSESS              AS train_sponsor_assess,
       a.TRAINEXPLAN                     AS train_explan,
       tao.`NAME`                        AS train_assist_organizer,
       tru.MSS_HRCODE                    as train_responsible_user,
       a.TRAINRESPONSIBLEUSERNAME        AS train_responsible_user_name,
       tpt.`NAME`                        AS train_address,
       IF(CHAR_LENGTH(a.TRAINADDRESSINFO) > 98, CONCAT(SUBSTRING(a.TRAINADDRESSINFO, 1, 98), '...'),
          a.TRAINADDRESSINFO)            AS train_address_info,
       a.TRAINRESPONSIBLEUSERMOBILE      AS train_responsible_user_mobile,
       substring(a.TRAINBEGTIME, 1, 10)  AS train_beg_time,
       substring(a.TRAINENDTIME, 1, 10)  AS train_end_time,
       substring(a.SIGNUPBEGTIME, 1, 10) AS signup_beg_time,
       substring(a.SIGNUPENDTIME, 1, 10) AS signup_end_time,
       a.TRAINCOST                       AS train_fee,
       ts.name                           AS training_status,
       b.MSSHRCODE                       AS sup_dept_code,
       a.SUPDEPTNAME                     AS sup_dept_name,
       b.MSSTYPE                         AS sup_dept_type,
       o.MSSHRCODE                       AS creat_plan_org,
       o.msstype                         AS creat_plan_org_type,
       o.MSSHRCODE                       AS org_id,
       '中国电信四川公司本部'                AS org_name,
       o.msstype                         AS org_class
FROM NU_TRAINSOURCEDATA_SC_HYK a
         LEFT JOIN MC_ORG_SHOW b ON a.supDeptCode = b.id
         LEFT JOIN MC_ORG_SHOW o ON a.ORGANIZERID = o.id
         left join mc_user_ztk tru on tru.ID = a.TRAINRESPONSIBLEUSER
         left join fz_train_traincategory tc on tc.`CODE` = a.TRAINCATEGORY
         left join fz_train_trainmode tm on tm.`CODE` = a.TRAINMODE
         left join fz_train_traintype tt on tt.`CODE` = a.TRAINTYPE
         left join fz_train_trainingplacetype tpt on tpt.`CODE` = a.ISDOMESTIC
         left join fz_train_trainstatus ts on ts.`CODE` = a.trainstatus
         left join fz_train_trainlevel tl on tl.`CODE` = a.TRAINLEVEL
         LEFT JOIN mc_org_show tao on tao.id = a.TRAINASSISTORGANIZER
WHERE 1 = 1