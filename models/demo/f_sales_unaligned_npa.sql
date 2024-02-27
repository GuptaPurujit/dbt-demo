{{ config(
	partition_by=['pt_data_dt', 'pt_run_id']
) }}

with npa_sales as (
	SELECT distinct Channel
	       ,Month
	       ,NRx
	       ,TRx
	       ,NBRx
	       ,prod_id
	FROM 
		{{ source('demo', 'f_account_sales_npa') }} 
),

product_xref as (
	SELECT distinct *
	FROM 
		{{ source('demo', 'd_product_xref') }} 
),

product_master as (
	SELECT  distinct prod_id
	       ,name
	       ,brand_name
	FROM 
		{{ source('demo', 'd_product_master') }} 
),

lookup_normalization as (
	SELECT  distinct product_id
	       ,normalization_factor
	FROM 
		{{ source('demo', 'm_lookup_normalization') }} 
),

product_hierarchy as (
	SELECT  distinct *
	FROM
		{{ source('demo', 'd_product_hierarchy') }}
),

f_sales_unaligned_npa as (
	SELECT  brand_name
		   ,upper(Channel) as Channel
		   ,CASE WHEN product_hierarchy.form_str_id is null THEN product_master.prod_id  ELSE product_hierarchy.form_str_id END  AS prod_id
		   ,CASE WHEN product_hierarchy.form_str_name is null THEN product_master.name  ELSE product_hierarchy.form_str_name END AS prod_name
		   ,form_name
		   ,family_name
		   ,market_name
		   ,Month as processing_date
		   ,NRx
		   ,coalesce(normalization_factor,1)*NRx                                    AS normalized_NRx
		   ,TRx
		   ,coalesce(normalization_factor,1)*TRx                                    AS normalized_TRx
		   ,NBRx
		   ,coalesce(normalization_factor,1)*NBRx                                   AS normalized_NBRx
		   ,current_date()															AS pt_data_dt
		   ,now()														            AS pt_run_id
	FROM
		npa_sales

	INNER JOIN
		product_xref
	ON npa_sales.prod_id = product_xref.alternate_id

	INNER JOIN
		product_master
	ON product_xref.prod_id = product_master.prod_id

	LEFT JOIN
		lookup_normalization
	ON product_xref.prod_id = lookup_normalization.product_id

	LEFT JOIN
		product_hierarchy
	ON product_xref.prod_id = product_hierarchy.package_product_id
)

select * from f_sales_unaligned_npa
