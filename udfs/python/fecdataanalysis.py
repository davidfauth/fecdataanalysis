from pig_util import outputSchema

# 
# This is where we write python UDFs (User-Defined Functions) that we can call from pig.
# Pig needs to know the schema of the data coming out of the function, 
# which we specify using the @outputSchema decorator.
#
COUNT = 1

@outputSchema('generateNodeType:int')
def generateNodeType(input_str):
    """
    Function that generates the group color for D3.JS force graph. Some SuperPacs will support Candidate A and oppose
    Candidate B
    """
    
    if input_str == 'Oppose_ROMNEY, MITT':
        return 1
    elif input_str == 'Support_ROMNEY, MITT':
        return 2
    elif input_str == 'Support_OBAMA, BARACK':
        return 7
    elif input_str == 'Oppose_OBAMA, BARACK':
        return 4
    else:
        return 12
        
@outputSchema('buildEndNode:int')
def buildEndNode(input_str):
    """
    A simple example function that just returns the length of the string passed in.
    """
    
    if input_str == 'ROMNEY, MITT':
        return 0
    elif input_str == 'OBAMA, BARACK':
        return 1  
    
@outputSchema('buildNodeString:chararray')
def buildNodeString(nodeString, nodeGroup):
	return ('{"name":"' + nodeString + '","group":' + str(nodeGroup) + '},')


@outputSchema('buildLinkNodeString:chararray')
def buildLinkNodeString(startNode, endNode, strength):
    """
    A simple example function that just returns the length of the string passed in.
    """
    return ('{"source":' + str(startNode) + ',"target":' + str(endNode) + ',"value":' + str(strength) + '},')
    
@outputSchema('auto_increment_id:int')
def auto_increment_id():
    global COUNT
    COUNT += 1
    return COUNT
