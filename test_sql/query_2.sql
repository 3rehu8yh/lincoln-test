-- Note that this request was written for SQLite.
select
  client_id,
  sum(prod_price * prod_qty) filter (where product_type = 'MEUBLE') as ventes_meuble,
  sum(prod_price * prod_qty) filter (where product_type = 'DECO') as ventes_deco
from transactions
inner join product_nomenclature
on product_nomenclature.product_id = transactions.prod_id
where strftime('%Y', date) = '2020'
group by client_id
-- The "order by" is not part of the spec but allows for a
-- deterministic ordering in the result dataframe
order by client_id
