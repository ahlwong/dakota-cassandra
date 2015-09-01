// node modules
var nmDakota = require('../../index');

module.exports = {
  
  // timestamps
  ctime: {
    validator: nmDakota.Recipes.Validators.required
  },
  utime: {
    validator: nmDakota.Recipes.Validators.required
  },
  
  // data
  id: {
    validator: nmDakota.Recipes.Validators.required
  },
  email: {
    displayName: 'Email',
    validator: nmDakota.Recipes.Validators.email,
    sanitizer: nmDakota.Recipes.Sanitizers.email
  },
  name: {
    displayName: 'Name',
    validator: [nmDakota.Recipes.Validators.required, nmDakota.Recipes.Validators.minLength(1)]
  }
  
};