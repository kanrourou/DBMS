
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    this->iter = input;
    this->iter->getAttributes(this->attrs);
    this->condition = condition;
}

RC Filter::freeMemory(void* page_dummy) {
	if (page_dummy == NULL)
		return -1;
	free(page_dummy);
	page_dummy = NULL;
	return 0;
}

RC Filter::getNextTuple(void* data) {
	int res = -1;
	while(true) {
		res = iter->getNextTuple(data);
		if(res) {
			return res;
		}
		if(isValid(data)) {
			return 0;
		} else {
			res = -1;
			continue;
		}
	}
	cout << "Filter::getNextTuple -> opps" << endl;
	return -1;
}

bool Filter::isValid(void* data) {
	// decode condition
	string attrName = condition.bRhsIsAttr ? condition.rhsAttr : condition.lhsAttr;
	Value attrValue = condition.rhsValue;
	// retrieve value
	unsigned size = attrs.size();
	unsigned offset = 0;
	unsigned length = sizeof(unsigned);
	Attribute attr;
	for(int i=0; i<size; i++) {
		attr = attrs[i];
        // might have some problem !!! table.attributeName ???
		if(attr.name.compare(attrName) == 0) {
			break;
		}
		if(attr.type == TypeInt || attr.type == TypeReal) {
			offset += sizeof(int);
            length = sizeof(int);
		} else {
			memcpy(&length, (char *) data + offset, sizeof(unsigned));
			offset += sizeof(unsigned) + length;
		}
	}
	// check validation
	if(attr.type != attrValue.type) {
		cout << "Filter::isValid -> opps" << endl;
	}
	// start compare
	float compare_res = 0;
	if (attr.type == TypeInt) {
		int value = 0;
		memcpy(&value, (char *) data + offset, sizeof(int));
		int conditionValue = 0;
		memcpy(&conditionValue, attrValue.data, sizeof(int));
		if(condition.bRhsIsAttr) {
			compare_res = (float) (conditionValue - value);
		} else {
			compare_res = (float) (value - conditionValue);
		}
	} else if (attr.type == TypeReal) {
		float value = 0;
		memcpy(&value, (char *) data + offset, sizeof(float));
		float conditionValue = 0;
		memcpy(&conditionValue, attrValue.data, sizeof(float));
		if(condition.bRhsIsAttr) {
			compare_res = conditionValue - value;
		} else {
			compare_res = value - conditionValue;
		}
	} else {
        memcpy(&length, (char *) data + offset, sizeof(unsigned));
		char* tempData = (char *) malloc(length + 1);
		if (tempData == NULL) {
			cout << "Filter::isValid -> malloc error" << endl;
			return false;
		}
		// get the attribute
		memcpy(tempData, (char *) data + offset + sizeof(unsigned), length);
		char tail = '\0';
		memcpy(tempData + length, &tail, sizeof(char));
		string value(tempData);
		int realValueSize = 0;
		memcpy(&realValueSize, attrValue.data, sizeof(unsigned));
		char* realValue = (char *) malloc(realValueSize + 1);
		memcpy(realValue, (char *) attrValue.data + sizeof(unsigned), realValueSize);
		memcpy(realValue + realValueSize, &tail, sizeof(char));
		string conditionValue(realValue);
		if(condition.bRhsIsAttr) {
			compare_res = (float) conditionValue.compare(value);
		} else {
			compare_res = (float) value.compare(conditionValue);
		}
		freeMemory(tempData);
		freeMemory(realValue);
	}
	switch (condition.op) {
	case EQ_OP:
		return compare_res == 0;
		break;
	case LT_OP:
		return compare_res < 0;
		break;
	case GT_OP:
		return compare_res > 0;
		break;
	case LE_OP:
		return compare_res <= 0;
		break;
	case GE_OP:
		return compare_res >= 0;
		break;
	case NE_OP:
		return compare_res != 0;
		break;
	default:
		return false;
		break;
	}
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs = this->attrs;
}


Project::Project(Iterator* input, const vector<string> &attrNames) {
	this->iter = input;
	this->iter->getAttributes(this->wholeAttrs);
	this->attrNames = attrNames;
}

RC Project::getNextTuple(void* data) {
	void* tuple = malloc(PAGE_SIZE); // how to initialized ??? in what size
    assert(tuple != NULL);
    RC res = -1;
	res = iter->getNextTuple(tuple);
    if(res) {
        return res;
    }
	int attrSize = attrNames.size();
	unsigned dataOffset = 0;
	unsigned attrOffset = 0;
	unsigned attrLength = 0;
	vector<unsigned> attrOffsets;
	vector<unsigned> attrLengths;
	mapAttrInfo(tuple, attrOffsets, attrLengths);
	for(int i=0;i<attrSize;i++) {
		attrOffset = attrOffsets[i];
		attrLength = attrLengths[i];
		memcpy((char *) data + dataOffset, (char *) tuple + attrOffset, attrLength);
		dataOffset += attrLength;
	}
    free(tuple);
    tuple = NULL;
	return 0;
}

void Project::mapAttrInfo(void* data, vector<unsigned> &offsets, vector<unsigned> &lengths) {
	// construct map
	map<string, vector<unsigned> > attrMap;
	unsigned offset = 0;
	for(int i=0;i<wholeAttrs.size();i++) {
		Attribute attr = wholeAttrs[i];
		string attrName = attr.name;
		vector<unsigned> value;
		value.push_back(offset);
		value.push_back(sizeof(unsigned));
		if(attr.type == TypeInt || attr.type == TypeReal) {
			offset += sizeof(unsigned);
		} else {
			unsigned length = 0;
			memcpy(&length, (char *) data + offset, sizeof(unsigned));
			offset += sizeof(unsigned) + length;
			value[1] = length + sizeof(unsigned);
		}
		attrMap.insert(pair<string, vector<unsigned> >(attrName, value));
	}
	//
	offsets.clear();
	lengths.clear();
	for(int i=0;i<attrNames.size();i++) {
		string attrName = attrNames[i];
		vector<unsigned> value = attrMap[attrName];
		// push_back() - add a new element at the end
		offsets.push_back(value[0]);
		lengths.push_back(value[1]);
	}
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	// need to return the projected attributes
    attrs.clear();
    // cout << "Projection::getAttributes -> wholeAttrs.size " << wholeAttrs.size() << endl;
    // cout << "Projection::getAttributes -> projectAttrs.size " << attrNames.size() << endl;
    // for(int i=0;i<wholeAttrs.size();i++) {
    //    cout << "Projection::getAttributes -> wholeAttrs.name - " << i << " - " << wholeAttrs[i].name << endl;
    // }
    for(int i=0;i<(int) attrNames.size();i++) {
        string temp = attrNames[i];
        // cout << "Projection::getAttributes -> projectAttr.name - " << i << " - " << temp << endl;
        // cout << "Projection::getAttributes -> projectAttr.name.size - " << i << " - " << temp.size() << endl;
        int j = 0;
        for(;j<(int) wholeAttrs.size();j++) {
            Attribute attr = wholeAttrs[j];
            int res = temp.compare(attr.name);
            // cout << "Projection::getAttributes -> wholeAttr.name.size - " << j << " - " << attr.name.size() << endl;
            // cout << "Projection::getAttributes -> string.compare - j = " << j << " - " << res << endl;
            if(res == 0) {
                attrs.push_back(attr);
                break;
            }
        }
        if(j == wholeAttrs.size()) {
            cout << "Project::getAttributes -> error" << endl;
        }
    }
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	this->iter = input;
	this->aggAttr = aggAttr;
	this->op = op;
	this->isEOF = false;
	this->iter->getAttributes(this->wholeAttrs);
}

void Aggregate::freeMemory(void* data) {
	if (data == NULL)
		return;
	free(data);
	data = NULL;
}

void Aggregate::getResult(void* res) {
	switch (op) {
	case MIN :
		getMIN(res);
		break;
	case MAX :
		getMAX(res);
		break;
	case SUM :
		getSUM(res);
		break;
	case AVG:
		getAVG(res);
		break;
	case COUNT :
		getCOUNT(res);
		break;
	default:
		break;
	}

}

void Aggregate::getMIN(void* res) {
	int minInt = INT_MAX;
	float minReal = FLT_MAX;
	void *tempData = malloc(300);
	assert(tempData != NULL);
	if(aggAttr.type == TypeInt) {
		while(!iter->getNextTuple(tempData)) {
			int tempVal = getInt(tempData);
			if(minInt > tempVal) {
				minInt = tempVal;
			}
		}
		memcpy(res, &minInt, sizeof(int));
	} else if(aggAttr.type == TypeReal){
		while(!iter->getNextTuple(tempData)) {
			float tempVal = getReal(tempData);
			if(minReal > tempVal) {
				minReal = tempVal;
			}
		}
		memcpy(res, &minReal, sizeof(float));
	} else {
		cout << "getMIN -> type error" << endl;
	}
	freeMemory(tempData);
}

void Aggregate::getMAX(void* res) {
	int maxInt = INT_MIN;
	float maxReal = FLT_MIN;
	void *tempData = malloc(300);
	assert(tempData != NULL);
	if(aggAttr.type == TypeInt) {
		while(!iter->getNextTuple(tempData)) {
			int tempVal = getInt(tempData);
			if(maxInt < tempVal) {
				maxInt = tempVal;
			}
		}
		memcpy(res, &maxInt, sizeof(int));
	} else if(aggAttr.type == TypeReal){
		while(!iter->getNextTuple(tempData)) {
			float tempVal = getReal(tempData);
			if(maxReal < tempVal) {
				maxReal = tempVal;
			}
		}
		memcpy(res, &maxReal, sizeof(float));
	} else {
		cout << "getMAX -> type error" << endl;
	}
	freeMemory(tempData);
}

void Aggregate::getSUM(void* res) {
	long sumInt = 0;
	double sumReal = 0;
	void *tempData = malloc(300);
	assert(tempData != NULL);
	if(aggAttr.type == TypeInt) {
		while(!iter->getNextTuple(tempData)) {
			int tempVal = getInt(tempData);
			sumInt += tempVal;
		}
		int value = (int) sumInt;
		memcpy(res, &value, sizeof(int));
	} else if(aggAttr.type == TypeReal){
		while(!iter->getNextTuple(tempData)) {
			float tempVal = getReal(tempData);
			sumReal += tempVal;
		}
		float value = (float) sumReal;
		memcpy(res, &value, sizeof(float));
	} else {
		cout << "getSUM -> type error" << endl;
	}
	freeMemory(tempData);
}

void Aggregate::getAVG(void* res) {
	long sumInt = 0;
	double sumReal = 0;
	unsigned count = 0;
	void *tempData = malloc(300);
	assert(tempData != NULL);
	if(aggAttr.type == TypeInt) {
		while(!iter->getNextTuple(tempData)) {
			int tempVal = getInt(tempData);
			sumInt += tempVal;
			count++;
		}
		int value = (int) (sumInt / count);
		memcpy(res, &value, sizeof(int));
	} else if(aggAttr.type == TypeReal){
		while(!iter->getNextTuple(tempData)) {
			float tempVal = getReal(tempData);
			sumReal += tempVal;
			count++;
		}
		float value = (float) (sumReal / count);
		memcpy(res, &value, sizeof(float));
	} else {
		cout << "getAVG -> type error" << endl;
	}
	freeMemory(tempData);
}

void Aggregate::getCOUNT(void* res) {
	unsigned count = 0;
	void* tempData = malloc(300);
	assert(tempData != NULL);
	while(!iter->getNextTuple(tempData)) {
		count++;
	}
    if(aggAttr.type == TypeInt) {
        memcpy(res, &count, sizeof(unsigned));
    } else if (aggAttr.type == TypeReal){
        float countFlt = (float) count;
        memcpy(res, &countFlt, sizeof(float));
    } else {
        cout << "Aggregate::getCOUNT -> error" << endl;
    }
	// free memory
	free(tempData);
	tempData = NULL;
}

int Aggregate::getInt(void *data) {
	unsigned offset = getOffset(data);
	int res = 0;
	memcpy(&res, (char *) data + offset, sizeof(int));
	return res;
}

float Aggregate::getReal(void *data) {
	unsigned offset = getOffset(data);
	float res = 0;
	memcpy(&res, (char *) data + offset, sizeof(float));
	return res;
}

unsigned Aggregate::getOffset(void *data) {
	unsigned offset = 0;
	for(int i=0;i<wholeAttrs.size();i++) {
		Attribute attr = wholeAttrs[i];
		if(attr.name.compare(aggAttr.name) == 0) {
			break;
		}
		if(attr.type == TypeInt || attr.type == TypeReal) {
			offset += sizeof(unsigned);
		} else {
			unsigned length = 0;
			memcpy(&length, (char *) data + offset, sizeof(unsigned));
			offset += sizeof(unsigned) + length;
		}
	}
	return offset;
}

RC Aggregate::getNextTuple(void *data) {
	if(isEOF) {
		return -1;
	} else {
        void* res = malloc(sizeof(int));
        assert(res != NULL);
        getResult(res);
		memcpy(data, res, sizeof(int));
		isEOF = true;
        freeMemory(res);
		return 0;
	}
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	Attribute res = {aggAttr.name, aggAttr.type, aggAttr.length};
    char* temp;
	unsigned offset = 0;
	switch (op) {
	case MIN : {
        temp = (char *) malloc(res.name.size()+6);
        assert(temp != NULL);
		char* opValue = "MIN";
		memcpy(temp, opValue, 3);
		offset += 3;
		break;
	}
	case MAX :{
        temp = (char *) malloc(res.name.size()+6);
        assert(temp != NULL);
		char* opValue = "MAX";
		memcpy(temp, opValue, 3);
		offset += 3;
		break;
	}
	case SUM :{
        temp = (char *) malloc(res.name.size()+6);
        assert(temp != NULL);
		char* opValue = "SUM";
		memcpy(temp, opValue, 3);
		offset += 3;
		break;
	}
	case AVG : {
        temp = (char *) malloc(res.name.size()+6);
        assert(temp != NULL);
		char* opValue = "AVG";
		memcpy(temp, opValue, 3);
		offset += 3;
		break;
	}
	case COUNT : {
        temp = (char *) malloc(res.name.size()+8);
        assert(temp != NULL);
		char* opValue = "COUNT";
		memcpy(temp, opValue, 5);
		offset += 5;
		break;
	}
    default: {
        temp = (char *) malloc(res.name.size()+6);
        assert(temp != NULL);
		break;
    }
	}
	// left bracket
	char left = '(';
	memcpy(temp + offset, &left, sizeof(char));
	offset++;
	//
	memcpy(temp + offset, aggAttr.name.c_str(), aggAttr.name.size());
	offset += aggAttr.name.size();
	// right bracket
	char right = ')';
	memcpy(temp + offset, &right, sizeof(char));
	offset++;
	// tail
	char tail = '\0';
	memcpy(temp + offset, &tail, sizeof(char));
	res.name = string(temp);
	// add result
    attrs.clear();
	attrs.push_back(res);
	// free memory
	// freeMemory((void *)temp);
	free(temp);
	temp = NULL;
}











