-- Note that this request was written for SQLite.
select
  strftime('%d-%m-%Y', date) as date,
  sum(prod_price * prod_qty) as ventes
from transactions
where strftime('%Y', date) = '2019'
group by date
