select (city = 'Paris') as correct_capital_of_france
from first.cities
where country = 'France';

select (city = 'Ottawa')  as correct_capital_of_canada
from first.cities
where country = 'Canada';
