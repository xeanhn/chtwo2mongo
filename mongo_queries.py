from pymongo import MongoClient
# from bson.json_util import dumps
import time

client = MongoClient('127.0.0.1', 27017)
db = client.CH2_Row

def query_1(): 
    unwind_stage = {"$unwind": "$o_orderline"}       
    match_stage = {"$match": {"o_orderline.ol_delivery_d": {"$gt": "2014-07-01 00:00:00"}}}
    group_stage = {"$group": {"_id": "$o_orderline.ol_number",
                    "sum_qty": {"$sum": "$o_orderline.ol_quantity"},
                    "sum_amount": {"$sum": "$o_orderline.ol_amount"},
                    "avg_qty": {"$avg": "$o_orderline.ol_quantity"},
                    "avg_amount": {"$avg": "$o_orderline.ol_amount"},
                    "count_order": {"$sum": 1}}
                }

    sort_stage = {"$sort": {"_id": 1}}

    project_stage = {"$project": {
        "_id": 0,
        "ol_number": "$_id",
        "sum_qty": "$sum_qty",
        "sum_amount": "$sum_amount",
        "avg_qty": "$avg_qty",
        "avg_amount": "$avg_amount",
        "count_order": "$count_order"
        }}

    pipeline = [unwind_stage, match_stage, group_stage, sort_stage, project_stage]
    
    start_time = time.time()
    cursor = db.orders.aggregate(pipeline)
    # print(dumps(cursor, indent=2))
    return time.time() - start_time

def query_2(): 
    add_stage = {"$addFields": {
        "s_suppkey": {
            "$mod": [{"$multiply": ["$s_w_id", "$s_i_id"]}, 10000]
        }
    }}

    lookup_supplier = {"$lookup" : {
        "from":"supplier", 
        "localField": "s_suppkey", 
        "foreignField":"su_suppkey", 
        "as": "supplier"
    }}
    unwind_supplier = {"$unwind": "$supplier"}

    lookup_nation = {"$lookup": {
        "from": "nation", 
        "localField": "supplier.su_nationkey", 
        "foreignField":"n_nationkey", 
        "as": "nation"
    }}
    unwind_nation = {"$unwind": "$nation"}

    lookup_region = {"$lookup": {
        "from":"region",
        "localField": "nation.n_regionkey",
        "foreignField":"r_regionkey",
        "as": "region"
    }}
    unwind_region = {"$unwind": "$region"}

    match_stage1 = {"$match": {"region.r_name": {"$regex": "^Europ.*"}}}

    group_stage = {"$group": {
        "_id": {"s_i_id": "$s_i_id"}, 
        "m_s_quantity": {"$min": "$s_quantity"}, 
        "data": {"$push": {"data":"$$ROOT"}}
    }}
    unwind_data = {"$unwind": "$data"}
    match_stage2 = {"$match": {"$expr": {"$eq": ["$data.data.s_quantity", "$m_s_quantity"]}}}
    lookup_item = {"$lookup" : {
        "from":"item", 
        "localField": "data.data.s_i_id", 
        "foreignField":"i_id", 
        "as": "item"
    }}
    unwind_item = {"$unwind": "$item"}
    match_stage3 = {"$match" : {"item.i_data": {"$regex": "^.*b$"}}}
    project_stage = {"$project": {
        "_id": 0, 
        "su_suppkey": "$data.data.s_suppkey", 
        "su_name": "$data.data.supplier.su_name", 
        "n_name":"$data.data.nation.n_name", 
        "i_id": "$item.i_id", 
        "i_name": "$item.i_name", 
        "su_address":"$data.data.supplier.su_address", 
        "su_phone": "$data.data.supplier.su_phone", 
        "su_comment":"$data.data.supplier.su_comment"
    }}
    sort_stage = {"$sort": {"n_name": 1, "su_name": 1, "i_id": 1}}
    limit_stage = {"$limit": 100}

    
    pipeline = [add_stage, lookup_supplier, unwind_supplier, lookup_nation, unwind_nation, lookup_region, unwind_region, match_stage1, group_stage, unwind_data, match_stage2, lookup_item, unwind_item, match_stage3, project_stage, sort_stage, limit_stage]
    start_time = time.time()
    cur = db.stock.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cur, indent=2))
    return time.time() - start_time 

def query_3(): 
    match_stage1 = { "$match": { "o_entry_d": { "$gt": "2017-03-15 00:00:00.000000" } } }
    lookup_customer = {"$lookup" : {"from": "customer", 
    "let": {"customer_id": "$o_c_id", "warehouse_id": "$o_w_id", "district_id": "$o_d_id"}, 
    "pipeline": [
        {
        "$match": {
            "$expr": {
                "$and": [ 
                    {"$eq": ["$c_id", "$$customer_id"]},
                    {"$eq": ["$c_w_id", "$$warehouse_id"]}, 
                    {"$eq": ["$c_d_id", "$$district_id"]}
                ]
            }
        }
    }
    ],
    "as": "customer"
    }}


    unwind_customer = {"$unwind": "$customer"}
    match_stage2 = { "$match": { "customer.c_state": { "$regex": "^A.*$" } } }

    lookup_neworder = {"$lookup" : {"from": "neworder", 
    "let": {"o_w_id": "$o_w_id", "o_d_id": "$o_d_id", "o_id": "$o_id"}, 
    "pipeline": [
        {
        "$match": {
            "$expr": {
                "$and": [ 
                    {"$eq": ["$no_w_id", "$$o_w_id"]},
                    {"$eq": ["$no_d_id", "$$o_d_id"]}, 
                    {"$eq": ["$no_o_id", "$$o_id"]}
                ]
            }
        }
    }
    ],
    "as": "neworder"
    }}

    unwind_neworder = {"$unwind": "$neworder"}

    group_stage = {"$group": {
        "_id": {"order_id": "$o_id", "warehouse": "$o_w_id", "district": "$o_d_id", "entry": "$o_entry_d"},
        "revenue" : {"$sum": {"$sum": "$o_orderline.ol_amount"}}
    }}
    sort_stage = {"$sort" : {"revenue": -1, "_id.entry": 1}}

    project_stage =  {
            "$project" : {
                "_id" : 0,
                "o_id": "$_id.order_id",
                "o_w_id": "$_id.warehouse",
                "o_d_id": "$_id.district",
                "revenue": "$revenue",
                "o_entry_d": "$_id.entry"
            }
        }

    pipeline = [match_stage1, lookup_customer, unwind_customer, match_stage2, lookup_neworder, unwind_neworder, group_stage, sort_stage, project_stage]
    
    start_time = time.time()
    cur = db.orders.aggregate(pipeline)
    # print(dumps(cur, indent=2))
    return time.time() - start_time

def query_4(): 
    match_stage1 = { "$match": { 
        "o_entry_d": { 
            "$gte": "2015-07-01 00:00:00.000000",
            "$lt": "2015-10-01 00:00:00.000000"
        }
    }}

    add_stage = {"$addFields" : {
        "added_week" : {
            "$dateToString": {
                "date": {
                    "$dateAdd" : {
                        "startDate" : {
                            "$dateFromString" : {
                                "dateString": "$o_entry_d"
                            }
                        }, 
                        "unit": "week", "amount": 1
                    }
                },
                "format" : "%Y-%m-%d %H:%M:%S"
            }
        }}}


    unwind_ol = {"$unwind": {
        "path": "$o_orderline"
    }}



    match_stage2 = { "$match": { 
        "$expr": {
            "$gte": ["$o_orderline.ol_delivery_d", "$added_week"]
        }
    }}

    group_stage1 = {"$group": {"_id": {"o_id": "$o_id", "o_w_id": "$o_w_id", "o_d_id": "$o_d_id"},
                            "o_ol_cnt": {"$first": "$o_ol_cnt"}}
                }
    group_stage2 = {"$group": {"_id": "$o_ol_cnt",
                            "order_count": {"$sum": 1}}
                }

    sort_stage = {"$sort": {"_id": 1}}

    project_stage = {"$project": {
        "_id": 0,
        "o_ol_cnt": "$_id",
        "order_COUNT": "$order_count"
        
    }}

    pipeline = [match_stage1, add_stage, unwind_ol, match_stage2, group_stage1, group_stage2, sort_stage, project_stage]
    
    start_time = time.time()
    cursor = db.orders.aggregate( pipeline, allowDiskUse = True )
    # print(dumps(cursor, indent=2))
    return time.time() - start_time

def query_5(): 
    match_stage = { "$match": 
                        { "r_name": { "$eq": "Asia" }}
                }

    lookup_nation = {"$lookup": {
                "from": "nation",
                "localField": "r_regionkey",
                "foreignField": "n_regionkey",
                "as": "nation"
                }}

    unwind_nation = {"$unwind": "$nation"}


    lookup_customer = {"$lookup" : {"from": "customer", 
                        "let": {"n_nationkey": "$nation.n_nationkey"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        "$$n_nationkey",
                                        {"$function": {
                                                "body": "function(inputString) { return inputString.codePointAt(0); }",
                                                "args": ["$c_state"],
                                                "lang": "js"
                                            }}
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "customer"
                        }}

    unwind_customer = {"$unwind": "$customer"}

    lookup_orders = {"$lookup" : {"from": "orders", 
                        "let": {"c_id": "$customer.c_id", "c_w_id": "$customer.c_w_id", "c_d_id": "$customer.c_d_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$$c_id", "$o_c_id"]}, 
                                        {"$eq": ["$$c_w_id", "$o_w_id"]},
                                        {"$eq": ["$$c_d_id", "$o_d_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "orders"
                        }}

    unwind_orders = {"$unwind": "$orders"}

    unwind_ol = {"$unwind": "$orders.o_orderline"}

    match_stage2 = { "$match": {
        "orders.o_entry_d": { 
            "$gte": "2016-01-01 00:00:00.000000",
            "$lt": "2017-01-01 00:00:00.000000" }
    }}

    lookup_stock = {"$lookup" : {"from": "stock", 
                        "let": {"ol_i_id": "$orders.o_orderline.ol_i_id", "o_w_id": "$orders.o_w_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$s_i_id", "$$ol_i_id"]},
                                        {"$eq": ["$s_w_id", "$$o_w_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "stock"
                        }}

    unwind_stock = {"$unwind": "$stock"}
    add_stage = {"$addFields": {"s_suppkey": {"$mod": [{"$multiply": ["$stock.s_w_id", "$stock.s_i_id"]}, 10000]}}}

    lookup_supplier = {"$lookup": {
                "from": "supplier",
                "localField": "s_suppkey",
                "foreignField": "su_suppkey",
                "as": "supplier"
                }}

    unwind_supplier = {"$unwind": "$supplier"}

    match_stage3 = {"$match": {
                        "$expr": {
                            "$eq": ["$supplier.su_nationkey", "$nation.n_nationkey"]
                    }}}

    group_stage = {"$group": {"_id": "$nation.n_name", 
                            "sum_ol_amount": {"$sum": "$orders.o_orderline.ol_amount"}
                        }}

    project_stage = {"$project": {
                        "n_name": "$_id",
                        "revenue": {"$round": ["$sum_ol_amount", 2]},
                        "_id": 0
                        }
                    }

    sort_stage = {"$sort" : {"revenue": -1}}

    pipeline = [match_stage, lookup_nation, unwind_nation, lookup_customer, unwind_customer, lookup_orders, unwind_orders, unwind_ol, match_stage2, lookup_stock, unwind_stock, add_stage, lookup_supplier, unwind_supplier, match_stage3, group_stage, project_stage, sort_stage]
    
    start_time = time.time()
    cursor = db.region.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time

def query_6(): 
    lookup_n2 =  {"$lookup" : {"from": "nation", 
                        "let": {"n1_name": "$n_name"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$or": [ 
                                        {"$and" : [{"$eq": ["$$n1_name", "Germany"]}, {"$eq": ["$n_name", "Cambodia"]}]},
                                        {"$and" : [{"$eq": ["$$n1_name", "Cambodia"]}, {"$eq": ["$n_name", "Germany"]}]}
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "n2"
                        }}

    unwind_n2 = {"$unwind" : "$n2"}

    lookup_customer =  {"$lookup" : {"from": "customer", 
                        "let": {"n_nationkey": "$n2.n_nationkey"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        "$$n_nationkey",
                                        {"$function": {
                                                "body": "function(inputString) { return inputString.codePointAt(0); }",
                                                "args": ["$c_state"],
                                                "lang": "js"
                                            }}
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "customer"
                        }}
    unwind_customer = {"$unwind" : "$customer"}
    lookup_orders = {"$lookup" : {"from": "orders", 
                        "let": {"c_id": "$customer.c_id", "c_w_id": "$customer.c_w_id", "c_d_id": "$customer.c_d_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$o_c_id", "$$c_id"]}, 
                                        {"$eq": ["$o_w_id", "$$c_w_id"]},
                                        {"$eq": ["$o_d_id", "$$c_d_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "orders"
                        }}
    unwind_orders = {"$unwind" : "$orders"}
    unwind_ol = {"$unwind" : "$orders.o_orderline"}
    match_stage1 =  { "$match": {
        "orders.o_orderline.ol_delivery_d": {
            "$gte": "2017-01-01 00:00:00.000000",
            "$lt": "2018-12-31 00:00:00.000000"
        }
    }}

    lookup_stock = {"$lookup" : {"from": "stock", 
                        "let": {"ol_i_id": "$orders.o_orderline.ol_i_id", "ol_w_id": "$orders.o_orderline.ol_supply_w_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$s_i_id", "$$ol_i_id"]},
                                        {"$eq": ["$s_w_id", "$$ol_w_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "stock"
                        }}
    unwind_stock = {"$unwind" : "$stock"}
    add_stage = {"$addFields": {"s_suppkey": {"$mod": [{"$multiply": ["$stock.s_w_id", "$stock.s_i_id"]}, 10000]}}}

    lookup_supplier = {"$lookup": {"from": "supplier",
                                "localField": "s_suppkey",
                                "foreignField": "su_suppkey",
                                "as": "supplier"
                        }}

    unwind_supplier = {"$unwind" : "$supplier"}

    match_stage2 = {"$match": {"$expr": {"$eq": ["$supplier.su_nationkey", "$n_nationkey"]}}}

    group_stage = {"$group" : {
        "_id" : {
            "supp_nation": "$supplier.su_nationkey",
            "cust_nation": {"$substr" : ["$customer.c_state", 0, 1]},
            "l_year" : { "$year": {"$dateFromString": { "dateString": "$orders.o_entry_d"}}}},
        "revenue" : {"$sum": "$orders.o_orderline.ol_amount"}
    }}

    sort_stage = {"$sort" : {"_id.supp_nation": 1, "_id.cust_nation": 1, "_id.l_year": 1}}

    project_stage = {"$project": {
                        "supp_nation": "$_id.supp_nation",
                        "cust_nation": "$_id.cust_nation",
                        "l_year": "$_id.l_year",
                        "revenue": {"$round": ["$revenue", 2]},
                        "_id": 0
                        }
                    }

    pipeline = [lookup_n2, unwind_n2, lookup_customer, unwind_customer, lookup_orders, unwind_orders, unwind_ol, match_stage1, lookup_stock, unwind_stock, add_stage, lookup_supplier, unwind_supplier, match_stage2, group_stage, sort_stage, project_stage]
    start_time = time.time()
    cur = db.nation.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cur, indent=2))
    return time.time() - start_time

def query_7(): 
    lookup_n2 =  {"$lookup" : {"from": "nation", 
                        "let": {"n1_name": "$n_name"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$or": [ 
                                        {"$and" : [{"$eq": ["$$n1_name", "Germany"]}, {"$eq": ["$n_name", "Cambodia"]}]},
                                        {"$and" : [{"$eq": ["$$n1_name", "Cambodia"]}, {"$eq": ["$n_name", "Germany"]}]}
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "n2"
                        }}

    unwind_n2 = {"$unwind" : "$n2"}

    lookup_customer =  {"$lookup" : {"from": "customer", 
                        "let": {"n_nationkey": "$n2.n_nationkey"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        "$$n_nationkey",
                                        {"$function": {
                                                "body": "function(inputString) { return inputString.codePointAt(0); }",
                                                "args": ["$c_state"],
                                                "lang": "js"
                                            }}
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "customer"
                        }}
    unwind_customer = {"$unwind" : "$customer"}
    lookup_orders = {"$lookup" : {"from": "orders", 
                        "let": {"c_id": "$customer.c_id", "c_w_id": "$customer.c_w_id", "c_d_id": "$customer.c_d_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$o_c_id", "$$c_id"]}, 
                                        {"$eq": ["$o_w_id", "$$c_w_id"]},
                                        {"$eq": ["$o_d_id", "$$c_d_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "orders"
                        }}
    unwind_orders = {"$unwind" : "$orders"}
    unwind_ol = {"$unwind" : "$orders.o_orderline"}
    match_stage1 =  { "$match": {
        "orders.o_orderline.ol_delivery_d": {
            "$gte": "2017-01-01 00:00:00.000000",
            "$lt": "2018-12-31 00:00:00.000000"
        }
    }}

    lookup_stock = {"$lookup" : {"from": "stock", 
                        "let": {"ol_i_id": "$orders.o_orderline.ol_i_id", "ol_w_id": "$orders.o_orderline.ol_supply_w_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$s_i_id", "$$ol_i_id"]},
                                        {"$eq": ["$s_w_id", "$$ol_w_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "stock"
                        }}
    unwind_stock = {"$unwind" : "$stock"}
    add_stage = {"$addFields": {"s_suppkey": {"$mod": [{"$multiply": ["$stock.s_w_id", "$stock.s_i_id"]}, 10000]}}}

    lookup_supplier = {"$lookup": {"from": "supplier",
                                "localField": "s_suppkey",
                                "foreignField": "su_suppkey",
                                "as": "supplier"
                        }}

    unwind_supplier = {"$unwind" : "$supplier"}

    match_stage2 = {"$match": {"$expr": {"$eq": ["$supplier.su_nationkey", "$n_nationkey"]}}}

    group_stage = {"$group" : {
        "_id" : {
            "supp_nation": "$supplier.su_nationkey",
            "cust_nation": {"$substr" : ["$customer.c_state", 0, 1]},
            "l_year" : { "$year": {"$dateFromString": { "dateString": "$orders.o_entry_d"}}}},
        "revenue" : {"$sum": "$orders.o_orderline.ol_amount"}
    }}

    sort_stage = {"$sort" : {"_id.supp_nation": 1, "_id.cust_nation": 1, "_id.l_year": 1}}

    project_stage = {"$project": {
                        "supp_nation": "$_id.supp_nation",
                        "cust_nation": "$_id.cust_nation",
                        "l_year": "$_id.l_year",
                        "revenue": {"$round": ["$revenue", 2]},
                        "_id": 0
                        }
                    }

    pipeline = [lookup_n2, unwind_n2, lookup_customer, unwind_customer, lookup_orders, unwind_orders, unwind_ol, match_stage1, lookup_stock, unwind_stock, add_stage, lookup_supplier, unwind_supplier, match_stage2, group_stage, sort_stage, project_stage]
    start_time = time.time()
    cur = db.nation.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cur, indent=2))
    return time.time() - start_time

def query_8(): 
    match_stage1 = { "$match": 
                        { "r_name": "Europe" }
                }

    lookup_nation = {"$lookup" : {
                "from":"nation",
                "localField": "r_regionkey",
                "foreignField":"n_regionkey",
                "as": "nation"
                }}

    unwind_nation = {"$unwind": "$nation"}



    lookup_customer = {"$lookup" : {"from": "customer", 
                        "let": {"n_nationkey": "$nation.n_nationkey"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": 
                                        {
                                            "$eq": [ {"$function": {
                                                        "body": "function(inputString) { return inputString.codePointAt(0); }",
                                                        "args": ["$c_state"],
                                                        "lang": "js"
                                            }},
                                            "$$n_nationkey"]},
                            }
                        }
                        ],
                        "as": "customer"
                        }}

    unwind_customer = {"$unwind": "$customer"}

    lookup_orders = {"$lookup" : {"from": "orders", 
                        "let": {"c_id": "$customer.c_id", "c_w_id": "$customer.c_w_id", "c_d_id": "$customer.c_d_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$$c_id", "$o_c_id"]}, 
                                        {"$eq": ["$$c_w_id", "$o_w_id"]},
                                        {"$eq": ["$$c_d_id", "$o_d_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "orders"
                        }}

    unwind_orders = {"$unwind": "$orders"}

    unwind_ol = {"$unwind": "$orders.o_orderline"}

    match_stage2 = { "$match": {"$and": [
        {"orders.o_entry_d": { 
            "$gte": "2017-01-01 00:00:00.000000", 
            "$lt": "2018-12-31 00:00:00.000000" 
        }},

        {"orders.o_orderline.ol_i_id": { "$lt": 1000 }} ,
    ]}}

    lookup_item = {"$lookup": {
                "from": "item",
                "localField": "orders.o_orderline.ol_i_id",
                "foreignField": "i_id",
                "as": "item"
                }}

    unwind_item = {"$unwind": "$item"}

    match_stage3 = {"$match": {
                        "item.i_data": {"$regex": "^.*b$"}
                }}

    lookup_stock = {"$lookup" : {"from": "stock", 
                        "let": {"ol_i_id": "$orders.o_orderline.ol_i_id", "ol_supply_w_id": "$orders.o_orderline.ol_supply_w_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$s_i_id", "$$ol_i_id"]},
                                        {"$eq": ["$s_w_id", "$$ol_supply_w_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "stock"
                        }}

    unwind_stock = {"$unwind": "$stock"}

    add_stage = {"$addFields": {"s_suppkey": {"$mod": [{"$multiply": ["$stock.s_w_id", "$stock.s_i_id"]}, 10000]}}}

    lookup_supplier = {"$lookup": {
                "from": "supplier",
                "localField": "s_suppkey",
                "foreignField": "su_suppkey",
                "as": "supplier"
                }}

    unwind_supplier = {"$unwind": "$supplier"}

    lookup_n2 = {"$lookup": {
                "from": "nation",
                "localField": "supplier.su_nationkey",
                "foreignField": "n_nationkey",
                "as": "n2"
                }}

    unwind_n2 = {"$unwind": "$n2"}

    add_case_amount_stage = {"$addFields": {"case_amount": {
                                            "$switch": {
                                                "branches": [{
                                                    "case": { "$eq": ["$n2.n_name", "Germany"]},
                                                    "then": "$orders.o_orderline.ol_amount"
                                                }],
                                                "default": 0
                                            }
                                    }
                        }}


    group_stage = {"$group": {
                    "_id": {"l_year": { "$year": {"$dateFromString": { "dateString": "$orders.o_entry_d"}}}},
                    "sum_case_amount": {"$sum": "$case_amount"},
                    "sum_ol_amount": {"$sum": "$orders.o_orderline.ol_amount"},
                }
    }

    sort_stage = {"$sort": {"_id.l_year": 1}}


    project_stage = {"$project": {
                        "l_year": "$_id.l_year",
                        "mkt_share": {"$round": [{"$divide": ["$sum_case_amount", "$sum_ol_amount"]}, 2]},
                        "_id": 0
                        }
                    }
    pipeline = [match_stage1, lookup_nation, unwind_nation, lookup_customer, unwind_customer, lookup_orders, unwind_orders, unwind_ol, match_stage2, lookup_item, unwind_item, match_stage3, lookup_stock, unwind_stock, add_stage, lookup_supplier, unwind_supplier, lookup_n2, unwind_n2, add_case_amount_stage, group_stage, sort_stage, project_stage]
    start_time = time.time()
    cursor = db.region.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cursor, indent=2))
    return time.time() - start_time

def query_9():
    lookup_item = {"$lookup": {
                "from": "item",
                "localField": "o_orderline.ol_i_id",
                "foreignField": "i_id",
                "as": "item"
                }}

    unwind_item = {"$unwind": "$item"}

    project_stage =  {
            "$project" : {
                "_id" : 0,
                "o_orderline" : {
                    "$filter" : {
                        "input" : "$o_orderline",
                        "cond" : {
                            "$eq": ["$$this.ol_i_id", "$item.i_id"]
                        }
                    }
                },
                "o_entry_d": 1,
                
                "item": 1
            }
        }

    match_stage = {"$match": {
                        "item.i_data": {"$regex": "^.*bb$"}
                }}

    unwind_ol = {"$unwind": "$o_orderline"}


    lookup_stock = {"$lookup" : {"from": "stock", 
                        "let": {"ol_i_id": "$o_orderline.ol_i_id", "ol_supply_w_id": "$o_orderline.ol_supply_w_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$s_i_id", "$$ol_i_id"]},
                                        {"$eq": ["$s_w_id", "$$ol_supply_w_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "stock"
                        }}

    unwind_stock = {"$unwind": "$stock"}


    add_stage = {"$addFields": {"s_suppkey": {"$mod": [{"$multiply": ["$stock.s_w_id", "$stock.s_i_id"]}, 10000]}}}

    lookup_supplier = {"$lookup": {
                "from": "supplier",
                "localField": "s_suppkey",
                "foreignField": "su_suppkey",
                "as": "supplier"
                }}

    unwind_supplier = {"$unwind": "$supplier"}

    lookup_nation = {"$lookup": {
                "from": "nation",
                "localField": "supplier.su_nationkey",
                "foreignField": "n_nationkey",
                "as": "nation"
                }}

    unwind_nation = {"$unwind": "$nation"}


    group_stage = {"$group": {
                    "_id": {"n_name": "$nation.n_name", "l_year": { "$year": {"$dateFromString": { "dateString": "$o_entry_d"}}}},
                    "SUM_profit": {"$sum": "$o_orderline.ol_amount"}
                }
    }

    sort_stage = {"$sort": {"_id.n_name" : 1, "_id.l_year": -1}}

    project_stage2 = {"$project": {
                        "n_name": "$_id.n_name",
                        "l_year": "$_id.l_year",
                        "SUM_profit": {"$round": ["$SUM_profit", 2]},
                        "_id": 0
                        }
                    }
    pipeline = [lookup_item, unwind_item, project_stage, match_stage, unwind_ol, lookup_stock, unwind_stock, add_stage, lookup_supplier, unwind_supplier, lookup_nation, unwind_nation, group_stage, sort_stage, project_stage2]
    
    start_time = time.time()
    cursor = db.orders.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cursor, indent=2))
    return time.time() - start_time

def query_10(): 
    match_stage = { "$match": { 
        "o_entry_d": { 
            "$gte": "2015-10-01 00:00:00.000000",
            "$lt": "2016-01-01 00:00:00.000000"
        }
    }}

    lookup_customer = {"$lookup" : {"from": "customer", 
                        "let": {"o_c_id": "$o_c_id", "o_w_id": "$o_w_id", "o_d_id": "$o_d_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$c_id", "$$o_c_id"]}, 
                                        {"$eq": ["$c_w_id", "$$o_w_id"]},
                                        {"$eq": ["$c_d_id", "$$o_d_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "customer"
                        }}

    unwind_customer = {"$unwind": "$customer"}

    add_stage = {"$addFields": {
                    "c_nationkey": {"$function": {
                        "body": "function(inputString) { return inputString.codePointAt(0); }",
                        "args": ["$customer.c_state"],
                        "lang": "js"
                    }}
                }
                }


    lookup_nation = {"$lookup": {
                "from": "nation",
                "localField": "c_nationkey",
                "foreignField": "n_nationkey",
                "as": "nation"
                }}

    unwind_nation = {"$unwind": "$nation"}
                
    group_stage = {"$group": {
                            "_id": {"c_id": "$customer.c_id", "c_last": "$customer.c_last", "c_city": "$customer.c_city", "c_phone": "$customer.c_phone", "n_name": "$nation.n_name", "n_nationkey": "$nation.n_nationkey", "c_state": "$nation.c_state"},
                            "revenue": {"$sum": {"$sum": "$o_orderline.ol_amount"}}
                }
    }
                
    sort_stage = {"$sort": {"revenue": -1}}

    limit_stage = {"$limit": 20}

    project_stage = {"$project": {
                        "c_id": "$_id.c_id",
                        "c_last": "$_id.c_last",
                        "revenue": "$revenue",
                        "c_city": "$_id.c_city",
                        "c_phone": "$_id.c_phone",
                        "n_name": "$_id.n_name",
                        "_id": 0
                        }
                    }

    pipeline = [ match_stage, lookup_customer, unwind_customer, add_stage, lookup_nation, unwind_nation, group_stage, sort_stage, limit_stage, project_stage]

    start_time = time.time()
    cursor = db.orders.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cursor, indent=2))
    return time.time() - start_time

def query_11(): 
    add_stage = {"$addFields": {"s_suppkey": {"$mod": [{"$multiply": ["$s_w_id", "$s_i_id"]}, 10000]}}}

    lookup_supplier = {"$lookup": {
                "from": "supplier",
                "localField": "s_suppkey",
                "foreignField": "su_suppkey",
                "as": "supplier"
                }}

    unwind_supplier = {"$unwind": "$supplier"}

    lookup_nation = {"$lookup": {
                "from": "nation",
                "localField": "supplier.su_nationkey",
                "foreignField": "n_nationkey",
                "as": "nation"
                }}

    unwind_nation = {"$unwind": "$nation"}

    match_stage = { "$match": {"nation.n_name": "Germany"}}

    group_stage = {"$group": {"_id": None,
                            "sum_s_order_cnt": {"$sum": "$s_order_cnt"},
                            "previous_data": {"$push": "$$ROOT"},
                    }}

    unwind_previous_data = {"$unwind": "$previous_data"}

    add_stage_2 = { "$addFields": {
                        "frac_sum_order_cnt": {
                            "$multiply": ["$sum_s_order_cnt", 0.00005]},
                    }}

    group_stage2 = {"$group": {"_id": "$previous_data.s_i_id",
                            "ordercount": {"$sum": "$previous_data.s_order_cnt"},
                            "frac_sum_order_cnt": {"$first": "$frac_sum_order_cnt"},
                    }}


    match_stage2 = {"$match":
                        {"$expr": {
                            "$gt": ["$ordercount", "$frac_sum_order_cnt"]
                        }}}

    sort_stage = {"$sort": {"ordercount": -1}}

    project_stage = {"$project": {
                        "s_i_id": "$_id",
                        "ordercount": "$ordercount",
                        "_id": 0
                        }
                    }

    pipeline = [add_stage, lookup_supplier, unwind_supplier, lookup_nation, unwind_nation, match_stage, group_stage, unwind_previous_data, add_stage_2, group_stage2, match_stage2, sort_stage, project_stage]
    start_time = time.time()
    cur = db.stock.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cur, indent=2))
    return time.time() - start_time

def query_12(): 
    unwind_stage = {"$unwind": "$o_orderline"}


    match_stage = { "$match": {
        "o_orderline.ol_delivery_d": { 
            "$gte": "2016-01-01 00:00:00.000000", 
            "$lt": "2017-01-01 00:00:00.000000"},
        "o_entry_d": { "$gte": "$o_orderline.ol_delivery_d"} 
    }
    }

    add_hl_and_ll_stage = {"$project": {"high_line": {
                                            "$switch": {
                                                "branches": [{
                                                    "case": {"$in": ["$o_carrier_id", [1,2]] },
                                                    "then": 1
                                                }],
                                                "default": 0
                                            }
                                        },
                                        "low_line": {
                                            "$switch": {
                                                "branches": [{
                                                    "case": {"$in": ["$o_carrier_id", [1,2]] },
                                                    "then": 0
                                                }],
                                                "default": 1
                                            }
                                        },
                                    "o_ol_cnt": 1},
                                    
                                    
                            }

    group_stage = {"$group": {"_id": "$o_ol_cnt",
                            "high_line_count": {"$sum": "$high_line"},
                            "low_line_count": {"$sum": "$low_line"}}
                    }

    sort_stage = {"$sort": {"_id": 1}}

    project_stage = {"$project": {
                        "o_ol_cnt": "$_id",
                        "high_line_count": "$high_line_count",
                        "low_line_count": "$low_line_count",
                        "_id": 0
                        }
                    }

    pipeline = [unwind_stage, match_stage, add_hl_and_ll_stage, group_stage, sort_stage, project_stage]
    start_time = time.time()
    cur = db.orders.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cur, indent=2))
    return time.time() - start_time


def query_13(): 
    lookup_orders = {"$lookup" : {"from": "orders", 
                        "let": {"customer_id": "$c_id", "warehouse_id": "$c_w_id", "district_id": "$c_d_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$o_w_id", "$$warehouse_id"]}, 
                                        {"$eq": ["$o_d_id", "$$district_id"]},
                                        {"$eq": ["$o_c_id", "$$customer_id"]}, 
                                        {"$gt": ["$o_carrier_id", 8]}, 
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "orders"
                        }}

    add_stage = {"$addFields" : {"o2_size" : {"$size" : "$orders"}}}

    group_stage1 = {"$group": {"_id": "$c_id", "c_count" : {"$sum": "$o2_size"}}}
    group_stage2 = {"$group": {"_id": "$c_count", "custdist": {"$sum": 1}}}
    sort_stage = {"$sort": {"custdist": -1, "_id": -1}}
    fake_sort = {"$sort": {"_id": 1}}

    project_stage = {"$project": {
                        "c_count": "$_id",
                        "custdist": "$custdist",
                        "_id": 0
                        }
                    }
    start_time = time.time()
    cur = db.customer.aggregate([lookup_orders, add_stage, group_stage1, group_stage2, sort_stage, project_stage], allowDiskUse = True)
    # print(dumps(cur, indent=2))
    return time.time() - start_time

def query_14(): 
    project_stage1 = {
            "$project" : {
                "_id" : 0,
                "o_orderline" : {
                    "$filter" : {
                        "input" : "$o_orderline",
                        "cond" : {
                            "$and" : [
                                {
                                    "$gte" : [
                                        "$$this.ol_delivery_d",
                                        "2017-09-01 00:00:00.000000"
                                    ]
                                },
                                {
                                    "$lt" : [
                                        "$$this.ol_delivery_d",
                                        "2017-10-01 00:00:00.000000"
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        }


    lookup_item = {"$lookup" : {"from": "item", 
                        "localField": "o_orderline.ol_i_id",
                        "foreignField": "i_id",
                        "as": "item"
                        }}

    unwind_item = {"$unwind": "$item"}

    project_stage2 = {
            "$project" : {
                "_id" : 0,
                "o_orderline" : {
                    "$filter" : {
                        "input" : "$o_orderline",
                        "cond" : {
                            "$eq": ["$$this.ol_i_id", "$item.i_id"]
                        }
                    }
                },
                "item": 1
            }
        }
    add_pr_amount_stage = {"$addFields": {"pr_amount": {
                                            "$switch": {
                                                "branches": [{
                                                    "case": { "$regexMatch": {"input": "$item.i_data", "regex": "^pr.*$"}},
                                                    "then": {"$sum": "$o_orderline.ol_amount"}
                                                }],
                                                "default": 0
                                            }
                                    }
                        }}

    group_stage = {"$group": {
                    "_id": None,
                    "sum_pr_amount": {"$sum": "$pr_amount"},
                    "revenue": {"$sum": {"$sum": "$o_orderline.ol_amount"}},
                    }
                }

    project_stage3 = { "$project": {
                        "_id": 0,
                        "promo_revenue": {
                            "$multiply": [{
                                "$divide": [
                                    "$sum_pr_amount", {"$add": [1, "$revenue"]}
                                ]
                            }, 100.0]},
        
                    }}

    pipeline = [project_stage1, lookup_item, unwind_item, project_stage2, add_pr_amount_stage, group_stage, project_stage3]
    start_time = time.time()
    cur = db.orders.aggregate(pipeline, allowDiskUse = True)
    # print(dumps(cur, indent=2))
    return time.time() - start_time

def query_15():
    project_stage1 = {
        "$project" : {
            "_id" : 0,
            "o_orderline" : {
                "$filter" : {
                    "input" : "$o_orderline",
                    "cond" : {
                        "$and" : [
                            {
                                "$gte" : [
                                    "$$this.ol_delivery_d",
                                    "2018-01-01 00:00:00.000000"
                                ]
                            },
                            {
                                "$lt" : [
                                    "$$this.ol_delivery_d",
                                    "2018-04-01 00:00:00.000000"
                                ]
                            }
                        ]
                    }
                }
            }
        }
    }

    unwind_ol = {"$unwind": "$o_orderline"}

    lookup_stock = {"$lookup" : {"from": "stock", 
                        "let": {"ol_i_id": "$o_orderline.ol_i_id", "ol_supply_w_id": "$o_orderline.ol_supply_w_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$s_i_id", "$$ol_i_id"]}, 
                                        {"$eq": ["$s_w_id", "$$ol_supply_w_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "stock"
                        }}

    unwind_stock = {"$unwind": "$stock"}


    add_stage = {"$addFields": {"s_suppkey": {"$mod": [{"$multiply": ["$stock.s_w_id", "$stock.s_i_id"]}, 10000]}}}

    group_stage = {"$group" : {
                "total_revenue": {"$sum": "$o_orderline.ol_amount"},
                "_id": "$s_suppkey"
                }}

    group_stage2 = {"$group": {
                    "_id": None,
                    "previous_data": {"$push": "$$ROOT"},
                    "max_revenue": {"$max": "$total_revenue"}}}

    unwind_previous_data = {"$unwind": "$previous_data"}

    lookup_supplier = {"$lookup": {
                "from": "supplier",
                "localField": "previous_data._id",
                "foreignField": "su_suppkey",
                "as": "supplier"
                }}

    unwind_supplier = {"$unwind": "$supplier"}


    match_stage2 = {"$match": {
                        "$expr": {
                            "$eq": ["$max_revenue", "$previous_data.total_revenue"]
                    }}}
                
    project_stage = {"$project": {
                        "_id": 0,
                        "su_suppkey": '$supplier.su_suppkey',
                        "su_name": "$supplier.su_name",
                        "su_address": "$supplier.su_address",
                        "su_phone": "$supplier.su_phone",
                        "total_revenue": "$previous_data.total_revenue"
                        }}

    sort_stage = {"$sort": {"su_suppkey": 1}}
                
    pipeline = [project_stage1, unwind_ol, lookup_stock, unwind_stock, add_stage, group_stage, group_stage2, unwind_previous_data, lookup_supplier, unwind_supplier, match_stage2, project_stage, sort_stage]
    start_time = time.time()
    cur = db.orders.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time

def query_16():
    lookup_item = {"$lookup": {
    "from":"item",
    "localField": "s_i_id",
    "foreignField":"i_id",
    "as": "item"
    }}
    unwind_item = {"$unwind": "$item"}

    match_stage1 = {"$match": {
        "item.i_data": {
            "$not": {"$regex": "^zz.*$"}
        }
    }}


    add_stage1 = {"$addFields": {
        "s_suppkey": {
            "$mod": [{"$multiply": ["$s_w_id", "$s_i_id"]}, 10000]
        }
    }}
    lookup_supplier = {"$lookup": {
        "from": "supplier",
        "localField": "s_suppkey",
        "foreignField": "su_suppkey",
        "as": "supplier"}}

    lookup_supplier = {"$lookup" : {
        "from": "supplier", 
        "let": {"s_suppkey": "$s_suppkey"}, 
        "pipeline": [
        {
        "$match": {
            "$expr": {
                "$and": [
                    {"$regexMatch": {"input": "$su_comment", "regex": "^.*Customer.*Complaints.*$"}},
                    {"$eq": ["$$s_suppkey", "$su_suppkey"]}, 
                ]
            }
        }
        }
        ],
        "as": "supplier"
        }}

    match_stage2 = {"$match": {
        "supplier": {
            "$eq": []
        }
    }}

    add_stage2 = {"$addFields": {
        "brand": {"$substr": ["$item.i_data", 0, 3]}}}

    group_stage = {"$group" : {
        "_id": {
            "i_name":"$item.i_name",
            "brand":"$brand",
            "i_price":"$item.i_price"},
        "supplier_cnt" : {"$sum": 1}
    }}

    sort_stage = {"$sort": {"supplier_cnt": -1}}

    project_stage = {"$project": {
        "_id": 0,
        "i_name": "$_id.i_name",
        "brand": "$_id.brand",
        "i_price": "$_id.i_price",
        "supplier_cnt": "$supplier_cnt"
    }}

    pipeline = [lookup_item, unwind_item, match_stage1, add_stage1, lookup_supplier, match_stage2, add_stage2, group_stage, sort_stage, project_stage]
    start_time = time.time()
    cur = db.stock.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time

def query_17():
    lookup_item = {"$lookup": {
            "from": "item",
            "localField": "o_orderline.ol_i_id",
            "foreignField": "i_id",
            "as": "item"
            }}

    unwind_item = {"$unwind": "$item"}
        
    project_stage = {
            "$project" : {
                "_id" : 0,
                "o_orderline" : {
                    "$filter" : {
                        "input" : "$o_orderline",
                        "cond" : {
                            "$eq": ["$$this.ol_i_id", "$item.i_id"]
                        }
                    }
                },
                "item": 1
            }
        }

    match_stage = {"$match": { "item.i_data": {"$regex": "^.*b$"} }}

    unwind_ol = {"$unwind": "$o_orderline"}

    group_stage1 = {"$group" : {
        "_id": "$item.i_id",
        "average": {"$avg": "$o_orderline.ol_quantity"},
        "order_info": {"$addToSet": {"quantity" : "$o_orderline.ol_quantity", "amount": "$o_orderline.ol_amount"}}
    }}

    filter_stage = {"$project": {
        "_id": "$_id",
        "average": "$average",
        "order_info": {
            "$filter" : {
                "input": "$order_info",
                "as": "orders",
                "cond": {
                    "$lt": ["$$orders.quantity", "$average"]
                }
            }
        }
    }}

    unwind_total = {"$unwind" : "$order_info"}

    group_stage2 = {"$group" : {
        "_id": None,
        "total": {"$sum": "$order_info.amount"}
    }}

    project_stage2 = {"$project": {
        "_id": 0,
        "avg_yearly": {
            "$divide": ["$total", 2]
        }
    }}

    pipeline = [lookup_item, unwind_item, project_stage, match_stage, unwind_ol, group_stage1, filter_stage, unwind_total, group_stage2, project_stage2]
    start_time = time.time()
    cur = db.orders.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time
    
def query_18():
    lookup_customer = {"$lookup" : {"from": "customer", 
                      "let": {"customer_id": "$o_c_id", "warehouse_id": "$o_w_id", "district_id": "$o_d_id"}, 
                       "pipeline": [
                           {
                           "$match": {
                               "$expr": {
                                   "$and": [ 
                                       {"$eq": ["$c_id", "$$customer_id"]},
                                       {"$eq": ["$c_w_id", "$$warehouse_id"]}, 
                                       {"$eq": ["$c_d_id", "$$district_id"]}
                                   ]
                               }
                           }
                       }
                       ],
                       "as": "customer"
                      }}

    unwind_customer = {"$unwind": "$customer"}


    group_stage = {"$group": {"_id": {
                                "o_id": "$o_id",
                                "o_d_id": "$o_d_id", 
                                "o_w_id": "$o_w_id", 
                                "c_id": "$customer.c_id", 
                                "c_last" : "$customer.c_last",
                                "o_entry_d" :"$o_entry_d", 
                                "o_ol_cnt" : "$o_ol_cnt"
                            }, 
                        
                            "sum(ol_amount)": {"$sum": {"$sum": "$o_orderline.ol_amount"}}
                        }}

    match_stage = {"$match" : {"sum(ol_amount)" : {"$gt": 200}}}

    sort_stage = {"$sort" : {"sum(ol_amount)": -1}}
    limit_stage = {"$limit": 100}

    project_stage = {"$project": {
        "_id": 0,
        "c_last": "$_id.c_last",
        "c_id": "$_id.c_id",
        "o_id": "$_id.o_id",
        "o_entry_d": "$_id.o_entry_d",
        "o_ol_cnt": "$_id.o_ol_cnt",
        "sum(ol_amount)": "$sum(ol_amount)"
    }}

    pipeline = [lookup_customer, unwind_customer, group_stage, match_stage, sort_stage, limit_stage, project_stage]
    start_time = time.time()
    cur = db.orders.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time

def query_19():
    unwind_ol = {"$unwind": "$o_orderline"}

    lookup_item = {"$lookup" : {
        "from":"item",
        "localField": "o_orderline.ol_i_id",
        "foreignField":"i_id",
        "as": "item"
    }}

    unwind_item = {"$unwind": "$item"}

    match_stage = {"$match" : {
        "$expr" : {
        
            "$or": [
                {
                    "$and": [
                        {"$regexMatch": {"input": "$item.i_data", "regex": "^.*h$"}},
                        {"$gte": ["$o_orderline.ol_quantity", 7]},
                        {"$lte": ["$o_orderline.ol_quantity", 17]},
                        {"$gte": ["$item.i_price", 1]},
                        {"$lte": ["$item.i_price", 5]},
                        {"$or": [{"$eq": ["$o_w_id", 37]}, {"$eq": ["$o_w_id", 29]}, {"$eq": ["$o_w_id", 70]}]}
                    ]
                }, 
                {
                    "$and": [
                    {"$regexMatch": {"input": "$item.i_data", "regex": "^.*b$"}},
                        {"$gte": ["$o_orderline.ol_quantity", 16]},
                        {"$lte": ["$o_orderline.ol_quantity", 26]},
                        {"$gte": ["$item.i_price", 1]},
                        {"$lte": ["$item.i_price", 10]},
                        {"$or": [{"$eq": ["$o_w_id", 78]}, {"$eq": ["$o_w_id", 17]}, {"$eq": ["$o_w_id", 6]}]}
                    ]
                },
                {
                    "$and": [
                        {"$regexMatch": {"input": "$item.i_data", "regex": "^.*c$"}},
                        {"$gte": ["$o_orderline.ol_quantity", 24]},
                        {"$lte": ["$o_orderline.ol_quantity", 34]},
                        {"$gte": ["$item.i_price", 1]},
                        {"$lte": ["$item.i_price", 15]},
                        {"$or": [{"$eq": ["$o_w_id", 91]}, {"$eq": ["$o_w_id", 95]}, {"$eq": ["$o_w_id", 6]}]}
                    ]
                }
            ]
            
        }
    }}
    group_stage = {"$group": {"_id": None, "revenue": {"$sum": "$o_orderline.ol_amount"}}}

    project_stage = {"$project": {
                    "_id": 0,
                    "revenue": 1
    }}

    pipeline = [unwind_ol, lookup_item, unwind_item, match_stage, group_stage, project_stage]
    start_time = time.time()
    cur = db.orders.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time
    
def query_20():
    project_stage1 = {
        "$project" : {
            "_id" : 0,
            "o_orderline" : {
                "$filter" : {
                    "input" : "$o_orderline",
                    "cond" : {
                        "$and" : [
                            {
                                "$gte" : [
                                    "$$this.ol_delivery_d",
                                    "2016-01-01 12:00:00"
                                ]
                            },
                            {
                                "$lt" : [
                                    "$$this.ol_delivery_d",
                                    "2017-01-01 12:00:00"
                                ]
                            }
                        ]
                    }
                }
            }
        }
    }


    lookup_stock = {"$lookup": {
                "from": "stock",
                "localField": "o_orderline.ol_i_id",
                "foreignField": "s_i_id",
                "as": "stock"
                }}

    unwind_stock = {"$unwind": "$stock"}

    project_stage2 =  {
            "$project" : {
                "_id" : 0,
                "o_orderline" : {
                    "$filter" : {
                        "input" : "$o_orderline",
                        "cond" : {
                            "$eq": ["$$this.ol_i_id", "$stock.s_i_id"]
                        }
                    }
                },
                
                "stock": "$stock"
            }
        }

    lookup_item = {"$lookup": {
                "from": "item",
                "localField": "stock.s_i_id",
                "foreignField": "i_id",
                "as": "item"
                }}


    unwind_item = {"$unwind": "$item"}


    match_stage1 = {"$match": {
                        "item.i_data": {"$regex": "^co.*$"}
                }}


    group_stage = {"$group": {"_id": {
                                "s_i_id": "$stock.s_i_id", 
                                "s_w_id": "$stock.s_w_id", 
                                "s_quantity": "$stock.s_quantity",
                                
                            }, 
                            "total_quantity": {"$sum": {"$sum": "$o_orderline.ol_quantity"}}
                        }}


    match_stage2 = {"$match": {
                        "$expr": {
                            "$gt": [{"$multiply": [20, "$_id.s_quantity"]}, "$total_quantity"]
                    }}}

    add_stage = {"$addFields": {
        "s_suppkey": {
            "$mod": [{"$multiply": ["$_id.s_w_id", "$_id.s_i_id"]}, 10000]}
    }}

    lookup_supplier = {"$lookup": {
                "from": "supplier",
                "localField": "s_suppkey",
                "foreignField": "su_suppkey",
                "as": "supplier"
                }}

    unwind_supplier = {"$unwind": "$supplier"}


    lookup_nation = {"$lookup": {
                "from": "nation",
                "localField": "supplier.su_nationkey",
                "foreignField": "n_nationkey",
                "as": "nation"
                }}

    unwind_nation = {"$unwind": "$nation"}

    match_stage3 = {"$match": {"nation.n_name": "Germany"}}


    project_stage3 = {"$project": {
                        "_id": 0,
                        "su_name": "$supplier.su_name",
                        "su_address": "$supplier.su_address",
                        }}

    sort_stage = {"$sort" : {"su_name": 1}}

    pipeline = [project_stage1, lookup_stock, unwind_stock, project_stage2, lookup_item, unwind_item, match_stage1, group_stage, match_stage2, add_stage, lookup_supplier, unwind_supplier, lookup_nation, unwind_nation, match_stage3, project_stage3, sort_stage]
    start_time = time.time()
    cur = db.orders.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time

def query_21():
    match_stage1 = { "$match": { 
    "o_entry_d": { 
        "$gte": "2017-12-01 00:00:00", 
        "$lte": "2017-12-31 00:00:00"
    }
    }}

    add_stage1 = {"$addFields" : {
        "added_150_days" : {
            "$dateToString": {
                "date": {
                    "$dateAdd" : {
                        "startDate" : {
                            "$dateFromString" : {
                                "dateString": "$o_entry_d"
                            }
                        }, 
                        "unit": "day", "amount": 150
                    }
                },
                "format" : "%Y-%m-%d %H:%M:%S"
            }
        }}}

    unwind_ol = {"$unwind" : "$o_orderline"}

    match_stage2 = {"$match": {"$expr": {"$gt": ["$o_orderline.ol_delivery_d", "$added_150_days"]}}}

    lookup_stock = {"$lookup" : {"from": "stock", 
                        "let": {"ol_i_id": "$o_orderline.ol_i_id", "o_w_id": "$o_w_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$s_i_id", "$$ol_i_id"]},
                                        {"$eq": ["$s_w_id", "$$o_w_id"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "stock"
                        }}

    unwind_stock = {"$unwind" : "$stock"}

    add_stage2 = {"$addFields": {"s_suppkey": {"$mod": [{"$multiply": ["$stock.s_w_id", "$stock.s_i_id"]}, 10000]}}}

    lookup_supplier = {"$lookup": {"from": "supplier",
                                "localField": "s_suppkey",
                                "foreignField": "su_suppkey",
                                "as": "supplier"
                        }}

    unwind_supplier = {"$unwind" : "$supplier"}

    lookup_nation = {"$lookup": {"from": "nation",
                                "localField": "supplier.su_nationkey",
                                "foreignField": "n_nationkey",
                                "as": "nation"
                        }}

    unwind_nation = {"$unwind": "$nation"}

    match_stage3 = {"$match" : {"nation.n_name" : "Peru"}}

    lookup_o2 = {"$lookup" : {"from": "orders", 
                        "let": {"o_id": "$o_id", "o_w_id": "$o_w_id", "o_d_id": "$o_d_id", "o1_ol_delivery_d": "$o_orderline.ol_delivery_d"}, 
                        "pipeline": [
                            {"$unwind" : "$o_orderline"},
                            {
                            
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$o_id", "$$o_id"]},
                                        {"$eq": ["$o_w_id", "$$o_w_id"]},
                                        {"$eq": ["$o_d_id", "$$o_d_id"]}, 
                                        {"$gte": ["$o_entry_d", "2017-12-01 00:00:00"]},
                                        {"$lt": ["$o_entry_d", "2017-12-31 00:00:00"]},
                                        {"$gt": ["$o_orderline.ol_delivery_d", "$$o1_ol_delivery_d"]}
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "o2"
                        }}


    add_stage3 = {"$addFields" : {"o2_size" : {"$size" : "$o2"}}}


    group_stage1 = {"$group" : {"_id" : 
                                {"o_w_id" : "$o_w_id", 
                                "o_d_id" : "$o_d_id",
                                "o_id" : "$o_id",
                                "nationkey" : "$nation.n_nationkey", 
                                "suppkey" : "$supplier.su_suppkey", 
                                "s_w_id" : "$stock.s_w_id", 
                                "s_i_id" : "$stock.s_i_id", 
                                "su_name" : "$supplier.su_name"}, "count" : {"$sum": "$o2_size"}}}

    match_stage4 = {"$match" : {"count" : {"$eq" : 0}}}

    group_stage2 = {"$group" : {"_id" : "$_id.su_name", "numwait" : {"$sum" : 1}}}

    project_stage = {"$project": {
        "su_name": "$_id",
        "numwait": "$numwait",
        "_id": 0
    }}


    pipeline = [match_stage1, add_stage1, unwind_ol, match_stage2, lookup_stock, unwind_stock, add_stage2, lookup_supplier, unwind_supplier, lookup_nation, unwind_nation, match_stage3, lookup_o2, add_stage3, group_stage1, match_stage4, group_stage2, project_stage]
    start_time = time.time()
    cur = db.orders.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time
    
def query_22():
    match_stage1 = {"$match": {"$expr": {
        "$and": [
            {"$regexMatch": {
                "input": "$c_phone",
                "regex": "^[1-7].*$"}}, 
            {"$gt": ["$c_balance", 0]}
            ]
    }}}

    lookup_orders = {"$lookup" : {"from": "orders", 
                        "let": {"customer_id": "$c_id", "warehouse_id": "$c_w_id", "district_id": "$c_d_id"}, 
                        "pipeline": [
                            {
                            "$match": {
                                "$expr": {
                                    "$and": [ 
                                        {"$eq": ["$o_c_id", "$$customer_id"]},
                                        {"$eq": ["$o_w_id", "$$warehouse_id"]}, 
                                        {"$eq": ["$o_d_id", "$$district_id"]}, 
                                        {"$gte": ["$o_entry_d", "2013-12-01 00:00:00"]},
                                        {"$lt": ["$o_entry_d", "2013-12-31 00:00:00"]},
                                    ]
                                }
                            }
                        }
                        ],
                        "as": "orders"
                        }}

    add_stage1 = {"$addFields" : {"orders_size" : {"$size" : "$orders"}}}

    match_stage2 = {"$match" : {"orders_size" : 0}}

    group_stage1 = {"$group": {
        "_id": None,
        "data": {"$push": {"data":"$$ROOT"}},
        "average": {"$avg":"$c_balance"}
    }}

    unwind_data = {"$unwind": "$data"}

    match_stage3 = {"$match" : {"$expr": {"$gt": ["$data.data.c_balance", "$average"]}}}

    add_stage2 = {"$addFields": {"country": {"$substr" : ["$data.data.c_state", 0, 1]}}}

    group_stage2 = {"$group": {
        "_id": "$country",
        "numcust": {"$sum": 1},
        "totacctbal": {"$sum": "$data.data.c_balance"}
    }}

    project_stage = {"$project": {
                        "_id": 0,
                        "country": "$_id",
                        "numcust": "$numcust",
                        "totacctbal": "$totacctbal"
                        }}

    sort_stage = {"$sort": {"country": 1}}

    
    pipeline = [match_stage1, lookup_orders, add_stage1, match_stage2, group_stage1, unwind_data, match_stage3, add_stage2, group_stage2, project_stage, sort_stage]
    start_time = time.time()
    cur = db.customer.aggregate(pipeline, allowDiskUse = True)
    return time.time() - start_time



if __name__ == "__main__":
    with open('results.txt', 'a') as file:
        file.write("QUERY 1: " + str(query_1()) + "\n") 
        file.write("QUERY 2: " + str(query_2()) + "\n")
        file.write("QUERY 3: " + str(query_3()) + "\n")
        file.write("QUERY_4: " + str(query_4()) + "\n")
        file.write("QUERY_5: " + str(query_5()) + "\n")
        file.write("QUERY_6: " + str(query_6()) + "\n")
        file.write("QUERY 7: " + str(query_7()) + "\n")
        file.write("QUERY 8: " + str(query_8()) + "\n")
        file.write("QUERY_9: " + str(query_9()) + "\n")
        file.write("QUERY_10: " + str(query_10()) + "\n")
        file.write("QUERY_11: " + str(query_11()) + "\n")
        file.write("QUERY_12: " + str(query_12()) + "\n")
        file.write("QUERY_13: " + str(query_13()) + "\n")
        file.write("QUERY_14: " + str(query_14()) + "\n")
        file.write("QUERY_15: " + str(query_15()) + "\n")
        file.write("QUERY_16: " + str(query_16()) + "\n")
        file.write("QUERY_17: " + str(query_17()) + "\n")
        file.write("QUERY_18: " + str(query_18()) + "\n")
        file.write("QUERY_19: " + str(query_19()) + "\n")
        file.write("QUERY_20: " + str(query_20()) + "\n")
        file.write("QUERY_21: " + str(query_21()) + "\n")
        file.write("QUERY_22: " + str(query_22()) + "\n")


