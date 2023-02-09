// Task 1iv

db.ratings.aggregate([
    // TODO: Write your query here
    // Task1: get all ratings of userID: 186
    { 
        $match: {
            userId: 186
        }
    },
    // Task2: get recent 5 rattings
    // Task2i: Sort the result based on the time stamps in desc high up 
    { 
        $sort: { 
            timestamp: -1 
        } 
    },
    // Task2ii: get 5
    { 
        $limit: 5 
    },
    // Task3: create the group with _id null so it affect the current recs
    // remeber push is like addAll in java
    {
        $group:
        {
            _id: null,
            movieIds: {
                $push: "$movieId"
            },
            ratings: {
                $push: "$rating"
            },
            timestamps: {
                $push: "$timestamp"
            }
        }
    },
    {
        $project: {
            _id:0, 
            movieIds: 1, 
            ratings: 1, 
            timestamps: 1
        }
    }
]);