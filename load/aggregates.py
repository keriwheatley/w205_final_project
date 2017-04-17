# 
# aggregates.py
# All functions that perform aggregations on the data lake must be defined in this file
# 
# if a function needs parameters from the config file, it should be able to handle a list
# as the single input parameter
# that list should be parsed within the function
# 



# test function
def testAgg( stuff):
    """Simple test to check function loader is working"""
    print("I am a test aggregation function!!")
    if len(stuff) > 0:
        print
        for s in stuff:
            print s,
        print '\n'



