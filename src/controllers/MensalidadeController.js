const MensalidadeModel  = require('../models/MensalidadeModel');


exports.getMensalidade = async (req, res) => {
    const { id } = req.params;
  
    try {
      const response = await MensalidadeModel .getBeneficiarioMensalidade(id);
  
      if (response.error) {
        return res.status(500).json({ message: 'Erro ao buscar mensalidades.', error: response.message });
      }
  
      if (!response.data) {
        return res.status(404).json({ message: 'Mensalidades não encontradas.' });
      }
  
      return res.status(200).json(response.data);
    } catch (error) {
      return res.status(500).json({ message: error.message });
    }
  };