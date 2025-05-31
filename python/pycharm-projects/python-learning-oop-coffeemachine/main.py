from menu import Menu, MenuItem
from coffee_maker import CoffeeMaker
from money_machine import MoneyMachine


menu = Menu()
moneymachine = MoneyMachine()
coffeemaker = CoffeeMaker()

moneymachine.report()
coffeemaker.report()
menu.get_items()

is_on = True

while is_on == True:
    options = menu.get_items()
    choice = input(f"What would you like?  {options} :")
    if choice == "off":
        is_on = False
    elif choice == "report":
        coffeemaker.report()
        moneymachine.report()
    else:
        drink = menu.find_drink(choice)
        if coffeemaker.is_resource_sufficient(drink):
            print(f"processing payment...")
            if moneymachine.make_payment(drink.cost):
                coffeemaker.make_coffee(drink)









