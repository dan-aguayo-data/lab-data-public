{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'table'
    )
}}



SELECT
    d1.c1 AS EMPLOYEE_NAME,
    d1.c2 AS EXPENSE_LINE_ID,
    d1.c3 AS EXPENSE_START_DATE,
    d1.c4 AS EXPENSE_TYPE,
    d1.c5 AS TRIP_DISTANCE,
    d1.c6 AS LEDGER_NAME
FROM
    (
        SELECT DISTINCT
            t4422506.c284646344 AS c1,
            t4422506.c491889612 AS c2,
            t4422506.c128557922 AS c3,
            t4422506.c378307667 AS c4,
            t4422506.c387870427 AS c5,
            t4422506.c10223035  AS c6
        FROM
            (
                SELECT
                    v486837407.full_name640            AS c284646344,
                    v300710610.expense_id              AS c491889612,
                    v300710610.start_date              AS c128557922,
                    v300710610.name2                   AS c378307667,
                    v300710610.trip_distance           AS c387870427,
                    v498378102.name496                 AS c10223035,
                    v300710610.expense_type_id1        AS pka_expensetypepeoexpensetype0,
                    v498378102.ledger_id               AS pka_ledgerid0,
                    v486837407.person_name_id564       AS pka_personnamepeopersonnameid0,
                    v486837407.effective_start_date574 AS pka_personnamepeoeffectivesta0,
                    v486837407.effective_end_date584   AS pka_personnamepeoeffectiveend0
                FROM
                    (
                        SELECT
                            exp.EXPENSE_ASSIGNMENT_ID AS assignment_id,
                            exp.EXPENSE_ID AS expense_id,
                            exp.EXPENSE_PERSON_ID AS person_id,
                            exp.EXPENSE_START_DATE AS start_date,
                            exp.EXPENSE_TRIP_DISTANCE AS trip_distance,
                            organizationinformationpeo.BUSINESS_UNIT_PRIMARY_LEDGER_ID AS primary_ledger_id,   
                            expensetypepeo.EXPENSE_TYPE_ID              AS expense_type_id1,
                            expensetypepeo.EXPENSE_TYPE_NAME                         AS name2
                        FROM
                            CES_ERP.FSCM_PROD_FINEXTRACT_EXMBICCEXTRACT.EXPENSE_EXTRACT_PVO    exp,  -- exm_expenses
                            CES_ERP.FSCM_PROD_FINEXTRACT_FUNBICCEXTRACT.BUSINESS_UNIT_EXTRACT_PVO    businessunitpeo,  
                            CES_ERP.FSCM_PROD_FINEXTRACT_EXMBICCEXTRACT.EXPENSE_TYPE_EXTRACT_PVO    expensetypepeo,  --exm_expense_types
                            CES_ERP.FSCM_PROD_FINFUNBUSINESSUNITS.BUSINESS_UNIT_PVO organizationinformationpeo  --hr_organization_information_f                            
                     WHERE
                            ( exp.EXPENSE_ORG_ID = businessunitpeo.FUN_BU_PERF_PEOBUSINESS_UNIT_ID
                              AND exp.EXPENSE_TYPE_ID = expensetypepeo.EXPENSE_TYPE_ID (+)
                              AND businessunitpeo.FUN_BU_PERF_PEOBUSINESS_UNIT_ID = organizationinformationpeo.ORGANIZATION_INFORMATION_ORGANIZATION_ID (+)
                              AND ( 'FUN_BUSINESS_UNIT' ) = organizationinformationpeo.ORG_UNIT_CLASSIFICATION_CLASSIFICATION_CODE (+)
                              AND ( SYSDATE() BETWEEN organizationinformationpeo.ORGANIZATION_INFORMATION_EFFECTIVE_START_DATE (+) AND organizationinformationpeo.ORGANIZATION_INFORMATION_EFFECTIVE_END_DATE (+) ) 
                              )
                            AND ( ( ( ( ( nvl(exp.EXPENSE_ITEMIZATION_PARENT_EXPENSE_ID, 1) ) <> - 1 ) ) )
                                  
                             --             OR hrc_session_util.get_user_personid = exp.EXPENSE_PERSON_ID
                             --             OR EXISTS (
                             --   SELECT
                             --       person_id
                             --   FROM
                             --       per_manager_hrchy_dn
                             --   WHERE
                             --           manager_id = hrc_session_util.get_user_personid
                             --       AND person_id = exp.EXPENSE_PERSON_ID
                             -- ) 
                            )
                    ) v300710610,
                    (
                        SELECT 
                            ledger.LEDGER_LEDGER_ID as LEDGER_ID, 
                            ledger.LEDGER_NAME AS name496
                        FROM
                            CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO ledger  --gl_ledgers
                        WHERE
                            ( ( ledger.LEDGER_OBJECT_TYPE_CODE = 'L' ) )
                    ) v498378102,
                    (
                        SELECT 
                            personpeo.PERSON_PEOPERSON_ID                AS person_id271,
                            personnamepeo.PERSON_NAME_ID       AS person_name_id564,
                            personnamepeo.EFFECTIVE_START_DATE AS effective_start_date574,
                            personnamepeo.EFFECTIVE_END_DATE   AS effective_end_date584,
                            personnamepeo.PERSON_NAME_PEOFULL_NAME            AS full_name640,
                            assignmentpeo.ASSIGNMENT_PEOASSIGNMENT_ID        AS assignment_id776
                        FROM
                            CES_ERP.FSCM_PROD_PERSON.PERSON_NAME_PVOVIEW_ALL  personpeo,  --per_persons
                            CES_ERP.FSCM_PROD_PERSON.PERSON_NAME_PVOVIEW_ALL  personnamepeo,  --per_person_names_f_v
                            CES_ERP.FSCM_PROD_PERSON.GLOBAL_PERSON_PVOVIEW_ALL assignmentpeo  --per_all_assignments_m
                        WHERE
                            ( personpeo.PERSON_PEOPERSON_ID = personnamepeo.PERSON_NAME_PEOPERSON_ID
                              AND personpeo.PERSON_PEOPERSON_ID = assignmentpeo.ASSIGNMENT_PEOPERSON_ID
                              AND ( SYSDATE() BETWEEN personnamepeo.EFFECTIVE_START_DATE AND personnamepeo.EFFECTIVE_END_DATE )
                              AND ( SYSDATE() BETWEEN assignmentpeo.ASSIGNMENT_PEOEFFECTIVE_START_DATE AND assignmentpeo.ASSIGNMENT_PEOEFFECTIVE_END_DATE ) )  
                           AND ( ( ( assignmentpeo.ASSIGNMENT_PEOEFFECTIVE_LATEST_CHANGE = 'Y' ) ) 
                                  AND ( ( ( ( assignmentpeo.ASSIGNMENT_PEOASSIGNMENT_TYPE = 'E' ) )
                                          OR ( ( assignmentpeo.ASSIGNMENT_PEOASSIGNMENT_TYPE = 'C' ) )
                                          OR ( ( assignmentpeo.ASSIGNMENT_PEOASSIGNMENT_TYPE = 'N' ) )
                                          OR ( ( assignmentpeo.ASSIGNMENT_PEOASSIGNMENT_TYPE = 'P' ) ) ) ) )
                    ) v486837407
                WHERE
                    ( v300710610.primary_ledger_id = v498378102.ledger_id
                      AND v300710610.person_id = v486837407.person_id271 (+)
                      AND v300710610.assignment_id = v486837407.assignment_id776 (+) )
                    AND ( ( ( v498378102.name496 = 'Scheme Ledger AU' ) )
                          AND ( ( v300710610.name2 = 'Km Reimbursement' ) ) )
            ) t4422506
    ) d1



              