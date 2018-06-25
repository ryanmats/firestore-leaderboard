from flask import Flask, redirect, render_template, request
from google.cloud import firestore


app = Flask(__name__)

@app.route('/')
def dashboard():
    # Redirect to the /play route
    return render_template('dashboard.html')

@app.route('/retrieve_rank', methods=['GET', 'POST'])
def retrieve_rank():
    player = request.form['player']
    firestore_client = firestore.Client()

    # Retrieve player score from players table
    player_ref = firestore_client.collection(u'players').document(player.encode("utf-8"))
    player_snapshot = player_ref.get()
    player_score = player_snapshot.get(u'maxScore')

    # Retrieve player rank using scores table
    scores_ref = firestore_client.collection(u'scores')
    query = scores_ref.where(u'score', u'>', player_score)
    score_docs = query.get()

    higher_ranked_players = 0
    for doc in score_docs:
        number_of_players = doc.get(u'numberOfPlayers')
        higher_ranked_players = higher_ranked_players + number_of_players

    player_rank = higher_ranked_players + 1

    return render_template('dashboard.html', player=player, player_rank=player_rank)

@app.route('/add_score', methods=['GET', 'POST'])
def add_score():
    player = request.form['player']
    score = int(request.form['score'])
    firestore_client = firestore.Client()

    # Update players table with new score
    transaction = firestore_client.transaction()
    player_ref = firestore_client.collection(u'players').document(player.encode("utf-8"))
    new_max_score = False
    
    @firestore.transactional
    def update_in_transaction(transaction, player_ref):
        snapshot = player_ref.get(transaction=transaction)
        existing_max_score = snapshot.get(u'maxScore')
        if score > existing_max_score:
            transaction.update(player_ref, {
                u'maxScore': score
            })
        return existing_max_score

    old_max_score = update_in_transaction(transaction, player_ref)
    if score > old_max_score:
        new_max_score = True
        print(u'Max score updated!')
    else:
        print(u'No new maximum score!')

    
    # If there is a new maximum score for a player, update the inverted index data structure.
    if new_max_score:
        print 'Updating inverted index!'
        transaction = firestore_client.transaction()
        old_max_score_ref = firestore_client.collection(u'scores').document(str(old_max_score).encode("utf-8"))
        new_max_score_ref = firestore_client.collection(u'scores').document(str(score).encode("utf-8"))

        @firestore.transactional
        def update_inverted_index_transaction(transaction, old_max_score_ref, new_max_score_ref):

            # Read old numberOfPlayers values from inverted index for old score / new score
            snapshot_old = old_max_score_ref.get(transaction=transaction)
            old_max_score_num_players = snapshot_old.get(u'numberOfPlayers')
            snapshot_new = new_max_score_ref.get(transaction=transaction)
            new_max_score_num_players = snapshot_new.get(u'numberOfPlayers')

            # Update scores table - decrement inverted index for old score
            transaction.update(old_max_score_ref, {
                u'numberOfPlayers': old_max_score_num_players - 1
            })

            # Update scores table - increment inverted index for new score
            transaction.update(new_max_score_ref, {
                u'numberOfPlayers': new_max_score_num_players + 1
            })

        update_inverted_index_transaction(transaction, old_max_score_ref, new_max_score_ref)
    
    return render_template('dashboard.html')

@app.errorhandler(500)
def server_error(e):
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

if __name__ == '__main__':
    # This is used when running locally with 'python main.py'
    app.run(host='127.0.0.1', port=8080, debug=True)
