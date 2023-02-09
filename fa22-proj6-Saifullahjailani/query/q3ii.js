// Task 3ii

db.credits.aggregate([
    // TODO: Write your query here
    // iterate over the array 
    //movieId (int): A unique identifier for the movie. Corresponds to the field of the same name in movies_metadata, keywords, and ratings
    //cast (array): 0 or more documents of the each containing the following fields:
        //id (int): A unique identifier for the actor who played the character
        //character (string): The name of the character in the movie
        //name (string): The name of the actor as listed in the movie's credits
    //crew (array): 0 or more documents each containing the following fields:
        //id (int): A unique identifier for the crew member
        //department (string): The department the crew member worked in
        //job (string): The job title of the crew member (e.g. "Audio Technician", "Director")
        //name (string): The name of the crew member as listed in the movie's credits
    { 
        $unwind: "$crew"
    },

    {
        $match:{
                'crew.id': 5655,
                'crew.job': "Director"
        }
    },
    { 
        $unwind: "$cast"
    },
    {
        $group: {
            // this is group by multiple columns, we should pass it as object
            _id: {id: "$cast.id", name: "$cast.name"},
            count: {$sum: 1}
        }
    },
    { 
        $project: {
            _id: 0, 
            id: "$_id.id", 
            name: "$_id.name", 
            count: 1}
    },
    { 
        $sort: {
            count: -1,
            id: 1
        }
    },
    { 
        $limit: 5
    }
]);