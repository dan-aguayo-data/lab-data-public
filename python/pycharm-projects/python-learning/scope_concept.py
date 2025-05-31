
##SCOPE: A variable that exists inside a function can only be accessed inside a function
#-- Everything you give a name to has a NameSpace and that NameSpace has a scope
# There is no block scope in Python... creating a varible in a while loop, for.. doesnt count as separate scope
#Summary: functions has their own scope. And  while,for, etc dont have their own scope. You can use function scope globally if global variable doesnt exist first.
# As a general rule avoid modifying global scope

enemies = 1
def increase_enemies():
    global enemies   ##this is how you get modify a global variable into a function
    enemies += 1
    print(f"enemies inside function: {enemies}")

increase_enemies()                             #this will give 2
print(f"enemies outside function: {enemies}")  # this will give 1

#LOCAL SCOPE: Local Scope exists within

#Local Scope

def drink_potion():
    potion_strength = 2
    print(potion_strength)

#Global Scope

player_health = 10


def drink_potion_2():
    potion_strength_2 = 2
    print(player_health)


def is_prime(num):
    num_test = num -1
    is_prime_number = True
    if num <= 1:
        is_prime_number = False
    elif num == 2:
        is_prime_number = True
    else:
        while num_test > 1 and num_test < num and is_prime_number == True:
            if num % num_test == 0:
                is_prime_number = False
            else:
                num_test = num_test - 1
    return print(is_prime_number)

is_prime(1)

#Gobal constants: variables you define and you don't want to change them ever again.
# convert them all upper separated with underscores case to differentiate them