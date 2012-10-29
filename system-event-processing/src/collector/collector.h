/*
 * collector.h
 *
 *  Created on: Oct 28, 2012
 *      Author: yxjiang
 */

#ifndef COLLECTOR_H_
#define COLLECTOR_H_

#include <boost/shared_ptr.hpp>

namespace event
{
/*!
 * Use shared pointer to store the large property_tree object
 */
typedef boost::shared_ptr<boost::property_tree::ptree> SharedPtree;




}


#endif /* COLLECTOR_H_ */
