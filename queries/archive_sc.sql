-- name: get_archive_sc_list
-- 获取四川人员培训档案信息
SELECT distinct a.ID                                                        as _id,
                a.ID                                                        AS id,
                a.DATASTATE                                                 AS operation,
                c.TRAINNAME                                                 AS training_name,
                substring(c.TRAINBEGTIME, 1, 10)                            AS training_start_date,
                substring(c.TRAINENDTIME, 1, 10)                            AS training_end_date,
                tc.`NAME`                                                   AS training_category,
                tm.`NAME`                                                   AS train_mode,
                d.msshrcode                                                 AS sponsor_org_id,
                d.msstype                                                   AS sponsor_org_type,
                e.msshrcode                                                 AS sponsor_dept,
                e.msstype                                                   AS sponsor_dept_type,
                tpt.`NAME`                                                  AS training_place_type,
                c.TRAINPURPOSE                                              AS train_purpose,
                c.TRAINCONTENT                                              AS train_content,
                c.UNDERTAKEORGNAME                                          as undertake_agent_name,
                ''                                                          as assist_agent_name,
                a.TRAINID                                                   AS training_id,
                IF(a.studyassessscore = '无评价', '', a.studyassessscore)   AS study_assess_score,
                IF(a.actionassessscore = '无评价', '', a.actionassessscore) AS action_assess_score,
                IF(a.trainingtime = '', '0', a.trainingtime)                AS training_time,
                a.cremark                                                   AS remark,
                a.CERTIFICATEID                                             AS certificate_id,
                a.certificatelevel                                          AS certificate_level,
                a.certificatename                                           AS certificate_name,
                a.grantdate                                                 AS grant_date,
                a.DEPT                                                      AS dept,
                a.deptcode                                                  AS dept_code,
                a.depttype                                                  AS dept_type,
                substring(a.GETCERTIFICATETIME, 1, 10)                      AS get_certificate_time,
                u.MSS_HRCODE                                                AS user_id,
                uo.msshrcode                                                AS user_org_id,
                uo.msstype                                                  AS user_org_type,
                u.MSS_JOBCATEGORY                                           AS job_category,
                IF(A.ISCOMPLETE = '1', '通过', '未通过')                    as psn_archive_status
FROM NU_TRAINUSERSOURCEDATA_SC_HYK a
         LEFT JOIN NU_TRAINSOURCEDATA_SC_HYK c ON a.TRAINID = c.id
         LEFT JOIN MC_ORG_SHOW d ON c.ORGANIZERID = d.id
         left join mc_org_show e on c.SUPDEPTCODE = e.id
         LEFT JOIN mc_user_ztk u ON a.USERID = u.id
         left join mc_org_show uo on uo.id = u.org
         left join fz_train_traincategory tc on tc.`CODE` = c.TRAINCATEGORY
         left join fz_train_trainmode tm on tm.`CODE` = c.TRAINMODE
         left join fz_train_traintype tt on tt.`CODE` = c.TRAINTYPE
         left join fz_train_trainingplacetype tpt on tpt.`CODE` = c.ISDOMESTIC
WHERE u.MSS_USERSTATUS = 1
  AND (u.MSS_JOBTYPE = 1 or u.MSS_HRJOBTYPE = 1)
  AND a.trainingtime > 0