# shebang omitted deliberately.

import twitter

def get_my_api():
    """
    Get the Twitter API, already authorized by Twitter.

    This function is probably not very secure.
    """

    # Cargo-culted from Russell's _Mining the Social Web_.
    CONSUMER_KEY = 'OGL1S2D1TNJg0fFFPYxW6YxLr'
    CONSUMER_SECRET = 'aBTLiUw3ure8eIimRta3S8j3yGL9tApTVMIT4VxhvF7E6cFJyZ'
    ACCESS_TOKEN = '3024132060-HkQUZMOoYsk1JugmJyZJlFGgWteWDyIbinoV4Jb'
    ACCESS_SECRET = 'j3wVpVR1s0pWrdjvd0aFpfVDTLPlC1xLc0tiHlioP2t89'

    auth = twitter.oauth.OAuth(ACCESS_TOKEN, ACCESS_SECRET,
                               CONSUMER_KEY, CONSUMER_SECRET)
    return twitter.Twitter(auth=auth)

if __name__ == "__main__":
    twitter_api = get_my_api()

    WORLD_WOE_ID = 1
    US_WOE_ID = 23424977
    JAPAN_WOE_ID = None
    world_trends = twitter_api.trends.place(_id=WORLD_WOE_ID)
    us_trends = twitter_api.trends.place(_id=US_WOE_ID)

    import json
    print(json.dumps(world_trends, indent=1))
    print()
    print(json.dumps(us_trends, indent=1))
    print()
    print(twitter_api)

