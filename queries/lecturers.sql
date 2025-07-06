-- name: get_lecturer_list
-- 获取讲师信息
SELECT
    a.ID AS _id,
    a.ID AS id,
    a.DATASTATE AS operation,
    a.trainid AS training_id,
    tt.`NAME` AS lecturer_type,
    a.`COURSEID` AS course_id,
    a.coursename AS course_name,
    a.COURSETIME AS course_time,
    case when a.COURSEASSESS is null then '5.00' else a.COURSEASSESS end as course_assess,
    date_format(a.startdate ,'%Y-%m-%d %H:%i:%s' ) AS start_date,
    date_format(a.enddate ,'%Y-%m-%d %H:%i:%s') AS end_date,
    b.MSS_HRCODE AS user_id,
    b.NAME_CARD_NAME AS user_name,
    b.MSS_JOBCATEGORY AS job_category
FROM
    nu_traincoursedata_xzs_hyk a
    INNER JOIN nu_trainsourcedata_xzs_hyk T ON T.id=A.TRAINID
    LEFT JOIN mc_user_ztk b ON a.userid = b.id
    LEFT JOIN fz_trainner_type tt on tt.`CODE`=a.LECTURERTYPE
    left join fz_train_traintype ttt on ttt.`CODE`=T.TRAINTYPE
WHERE
    a.COURSETIME > 0