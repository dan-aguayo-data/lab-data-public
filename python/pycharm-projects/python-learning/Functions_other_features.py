
#Simple Function with input
def my_function(name):
    print(f"Hello {name}")

my_function("Pedro")



#Functions with Outputs

def my_function_output():
    result = 3*2
    return result

my_function_output()
output = my_function_output()

print(f'{output}')


def format_name(fname,lname):
    formated_f_name = fname.title()
    formated_l_name  = lname.title()

    print(f"{formated_f_name} {formated_l_name}")
format_name("ANGELA","YU")

