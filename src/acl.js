import createDebug from 'debug';
import grpc from 'grpc';
import NodeAclCb from 'acl';
import jwtCb from 'jsonwebtoken';
import Promise from 'bluebird';

const debug = createDebug('acl');
const NodeAcl = Promise.promisifyAll(NodeAclCb);
const jwt = Promise.promisifyAll(jwtCb);
const acl = new NodeAcl(new NodeAcl.memoryBackend());

const jwtSecret = 'Ci23fWtahDYE3dfirAHrJhzrUEoslIxqwcDN9VNhRJCWf8Tyc1F1mqYrjGYF';
// Departments permissions
acl.allow([
  {
    roles: ['系统部'],
    allows: [
      { resources: 'reports/it', permissions: ['read', 'write', 'delete'] },
      { resources: 'users', permissions: ['read', 'write', 'delete'] },
    ] },
  {
    roles: ['市场部'],
    allows: [
      { resources: 'reports/marketing', permissions: ['read', 'write', 'delete'] },
    ] },
  {
    roles: ['交易部'],
    allows: [
      { resources: 'reports/trading', permissions: ['read', 'write', 'delete'] },
    ] },
  {
    roles: ['财务部'],
    allows: [
      { resources: 'reports/finance', permissions: ['read', 'write', 'delete'] },
    ] },
  {
    roles: ['总经办'],
    allows: [
      { resources: 'reports/management', permissions: ['read', 'write', 'delete'] },
    ] },
  {
    roles: ['客户'],
    allows: [
      { resources: 'reports/customers', permissions: ['read'] },
    ] },
]);

// Users permissions
acl.allow([
  {
    roles: ['victor'],
    allows: [
      { resources: 'getOrders', permissions: ['read', 'write', 'delete'] },
      { resources: 'funds/:fundid', permissions: ['read', 'write', 'delete'] },
    ] },
]);


// roles       {String|Array} Role(s) to check the permissions for.
// permissions {String|Array} asked permissions.
// resource    {String} resource to ask permissions for.
async function can(roles, permissions, resource) {
  try {
    const isRoleAuthorized = await acl.areAnyRolesAllowed(roles, resource, permissions);
    return isRoleAuthorized;
  } catch (error) {
    debug('can() Error: %o', error);
    throw error;
  }
}

async function grpcCan(ctx, permissions, resource) {
  try {
    const user = await jwt.verifyAsync(ctx.metadata.get('Authorization')[0], jwtSecret);

    const roles = user.dpt ? user.dpt.concat(user.userid) : [].concat(user.userid);
    debug(roles);

    const hasRight = await can(roles, permissions, resource);

    if (!hasRight) {
      throw new Error(`Roles: '${roles}' cannot '${permissions}' the '${resource}'.)`);
    }
    return user;
  } catch (error) {
    debug('grpcCan() Error: %o', error);
    const err = new Error('Access forbidden');
    err.code = grpc.status.UNAUTHENTICATED;
    throw err;
  }
}

export default grpcCan;
