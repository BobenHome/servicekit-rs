-- name: get_training_list
-- 获取人员培训清单信息
SELECT a.ID                                               AS _id,
       a.ID                                               AS id,
       a.DATASTATE                                        AS operation,
       a.trainid                                          AS training_id,
       b.MSS_HRCODE                                       AS user_id,
       IF(INSTR(a.STUDENTROLE, '5') > 0, 'true', 'false') AS is_sponsor,
       IF(a.graduationstate = '1', '通过', '未通过')      AS psn_training_status,
       b.MSS_JOBCATEGORY                                  AS job_category,
       c.TRAINEXPLAN                                      AS remark
FROM NU_TRAINUSERSOURCEDATA_xzs_hyk a
         INNER JOIN nu_trainsourcedata_xzs_hyk c ON a.TRAINID = c.id
         LEFT JOIN mc_user_ztk b ON a.userid = b.id
         LEFT JOIN fz_train_traintype tt ON tt.`CODE` = c.TRAINTYPE
WHERE b.MSS_USERSTATUS = 1
  AND (b.MSS_JOBTYPE = 1 OR b.MSS_HRJOBTYPE = 1)