
#List data structure
fruits = ["item1","item2","item3","item4"]

print(fruits)

#when order matters lists are good, the order is determined by the order of the list.
# #The order inside the variable is not lost


print(fruits[0])  #0 is the first item of the list
#the first item is at position 0, so and offset starts + 1+ 1 + 1

#you can also use a negative index
print(fruits[-1])   # -1 is the last item in the list


#you can change the values of a list

fruits[0] = "changed_item"

print(fruits)


#add an item using append

fruits.append("item5")
print(fruits)

# other list functions
# extend
# insert
# remove
#pop

fruits.extend(["item6","item7"])

print(fruits)


#number of items in the list, it will give you + 1 items
print(len(fruits) -1 )
number_items = len(fruits) -1

print(fruits[number_items])

#keep two lists under a variable in order
fruits =  ["strawberry","melon","grapes","berries"]
vegetables = ["onion","chilli","grass","weed"]

union_list = [fruits,vegetables]

print(union_list)

#the first indexing  will refer to the list number,
# and then the second one will refer to the value inside that list
print(union_list[0])
print(union_list[1])

print(union_list[1][2])
print(union_list[1][3])
print(union_list[1][1])