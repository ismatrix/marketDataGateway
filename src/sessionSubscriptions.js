import createDebug from 'debug';

const debug = createDebug('sessionSubscriptions.grpc');

const sessionSubsStore = {};

const matchSubscription = newSub => sub => (
  sub.instrument.symbol === newSub.instrument.symbol &&
  sub.resolution === newSub.resolution &&
  sub.dataType === newSub.dataType);

function add(sessionid, newSub) {
  try {
    debug('add() newSub: %o', newSub);
    if (!Array.isArray(sessionSubsStore[sessionid])) sessionSubsStore[sessionid] = [];

    const similarSubIndex = sessionSubsStore[sessionid].findIndex(matchSubscription(newSub));

    if (similarSubIndex === -1) {
      sessionSubsStore[sessionid].push(newSub);
    }
  } catch (error) {
    debug('Error add(): %o', error);
  }
}

function getAll() {
  try {
    debug('getAll()');
    const sessionids = Object.keys(sessionSubsStore);

    const allSubs = sessionids.reduce((acc, cur) => acc.concat(sessionSubsStore[cur]), []);
    return allSubs;
  } catch (error) {
    debug('Error getAll(): %o', error);
  }
}

function removeSub(sessionid, subToRemove) {
  try {
    if (sessionid in sessionSubsStore) {
      const similarSubIndex = sessionSubsStore[sessionid]
        .findIndex(matchSubscription(subToRemove));

      if (similarSubIndex !== -1) {
        const removedSub = sessionSubsStore.splice(similarSubIndex, similarSubIndex + 1);
        debug('removedSub %o', removedSub);
      }
    }
  } catch (error) {
    debug('Error removeSub(): %o', error);
  }
}

function removeSession(sessionid) {
  try {
    if (sessionid in sessionSubsStore) {
      delete sessionSubsStore[sessionid];
      debug('removeSession() removed: %o', sessionid);
    }
  } catch (error) {
    debug('Error removeSession(): %o', error);
  }
}

const sessionSubscriptions = {
  add,
  getAll,
  removeSub,
  removeSession,
};

export default sessionSubscriptions;
