// Task 3i

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
        $unwind: '$cast'
    },
    {
        $match:{
            'cast.id': 7624
        }
    },
    {
        $lookup: {
            from: "movies_metadata",
            localField: "movieId",
            foreignField: "movieId",
            as: "movie"
        }
    },
    {
        $unwind: '$movie'
    },
    {
        $project: {
            _id: 0,
            title: "$movie.title",
            release_date: "$movie.release_date",
            character: "$cast.character"
        }
    },
    { $sort: {release_date: -1}}

]);