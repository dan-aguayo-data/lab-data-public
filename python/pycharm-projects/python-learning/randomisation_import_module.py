import random
import my_fav_module ## bring variables from other module

#Marseme twister algorithm.
#Pseudo randon number generators.


#number between 1 to 10
random_test = random.randint(1,10)
print(random_test)
print(my_fav_module.my_fav_number)  ## bring variables from other module

#random floating numbers
random.random()  #floating number between 0 and 1. 1 is not included.
print(random.random()*100)

#random floating number point generator

print(random.uniform(1, 100)) #between two numbers inclusive of a and b


random_heads_or_tails = random.randint(0,1)

if random_heads_or_tails == 0:
    print("Heads")
else:
    print ("Tails")


#gets a random valuie from list
random_choice = random.choice(["item1","item2","item3"])

print(random_choice)

# Rock
print("""
    _______
---'   ____)
      (_____)
      (_____)
      (____)
---.__(___)
""")

# Paper
print("""
     _______
---'    ____)____
           ______)
          _______)
         _______)
---.__________)
""")

# Scissors
print("""
    _______
---'   ____)____
          ______)
       __________)
      (____)
---.__(___)
""")