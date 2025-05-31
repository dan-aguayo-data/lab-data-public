WITH source_reviews AS (

SELECT * FROM  {{ref("src_reviews")}}

)  

select * from source_reviews