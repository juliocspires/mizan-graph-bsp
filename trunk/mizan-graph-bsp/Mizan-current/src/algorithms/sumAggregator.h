/*
 * sumAggregator.h
 *
 *  Created on: Mar 6, 2013
 *      Author: refops
 */

#ifndef SUMAGGREGATOR_H_
#define SUMAGGREGATOR_H_

#include "../IAggregator.h"

class sumAggregator: public IAggregator<mLong> {
public:
	sumAggregator() {
		aggValue.setValue(0);
	}
	void aggregate(mLong value) {
		this->aggValue = mLong(this->aggValue.getValue() + value.getValue());
	}
	void createInitialValue(){
		aggValue.setValue(0);
	}
	mLong getValue() {
		return aggValue;
	}
	void setValue(mLong value) {
		this->aggValue = value;
	}
	virtual ~sumAggregator() {
	}
};

#endif /* SUMAGGREGATOR_H_ */
