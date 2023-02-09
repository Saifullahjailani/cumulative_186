// Task 1iii

db.ratings.aggregate([
    // TODO: Write your query here
    // group by ratings
    {
        $group: {
            _id: "$rating", // Group by the field ratings
            count: {
                $sum: 1
                } // Get the count for each group
        }
    },
    
    {
        $project: {
            _id: 0, rating: "$_id", count: 1
        }
    },
    {
        $sort: {
            rating: -1
        }
    }
]);