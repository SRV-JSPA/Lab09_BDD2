// 1. Gráfico de Evolución de Precios
[
    {
      $match: {
        "Fecha": { $exists: true },
        "Último": { $exists: true, $ne: null }
      }
    },
    {
      $sort: { "Fecha": 1 }
    },
    {
      $project: {
        _id: 0,
        fecha: "$Fecha",
        precio: "$Último"
      }
    }
  ]
  
  // 2. Gráfico de Velas 
  [
    {
      $match: {
        "Fecha": { $exists: true },
        "Apertura": { $exists: true },
        "Último": { $exists: true },
        "Máximo": { $exists: true },
        "Mínimo": { $exists: true }
      }
    },
    {
      $sort: { "Fecha": 1 }
    },
    {
      $project: {
        _id: 0,
        fecha: "$Fecha",
        apertura: "$Apertura",
        cierre: "$Último",
        maximo: "$Máximo",
        minimo: "$Mínimo",
        tendencia: {
          $cond: [
            { $gte: ["$Último", "$Apertura"] },
            "Alcista",
            "Bajista"
          ]
        }
      }
    }
  ]
  
  // 3. Gráfico de Volumen de Transacciones
  [
    {
      $match: {
        "Fecha": { $exists: true },
        "Volumen": { $exists: true },
        "Apertura": { $exists: true },
        "Último": { $exists: true }
      }
    },
    {
      $sort: { "Fecha": 1 }
    },
    {
      $project: {
        _id: 0,
        fecha: "$Fecha",
        volumen: "$Volumen",
        cierrePosNeg: { 
          $cond: [ 
            { $gt: ["$Último", "$Apertura"] }, 
            "Positivo", 
            "Negativo" 
          ] 
        }
      }
    }
  ]
  
  // 4. Histograma de Variaciones Diarias
  [
    {
      $match: {
        "Variacion": { $exists: true, $ne: null }
      }
    },
    {
      $project: {
        _id: 0,
        variacion: "$Variacion",
        rangoBin: {
          $switch: {
            branches: [
              { case: { $lt: ["$Variacion", -0.03] }, then: "< -3%" },
              { case: { $lt: ["$Variacion", -0.02] }, then: "-3% a -2%" },
              { case: { $lt: ["$Variacion", -0.01] }, then: "-2% a -1%" },
              { case: { $lt: ["$Variacion", 0] }, then: "-1% a 0%" },
              { case: { $lt: ["$Variacion", 0.01] }, then: "0% a 1%" },
              { case: { $lt: ["$Variacion", 0.02] }, then: "1% a 2%" },
              { case: { $lt: ["$Variacion", 0.03] }, then: "2% a 3%" },
            ],
            default: "> 3%"
          }
        },
        esPositivo: { $gte: ["$Variacion", 0] }
      }
    }
  ]
  
  // 5. KPI de Rendimiento Acumulado
  [
    {
      $sort: { "Fecha": 1 }
    },
    {
      $group: {
        _id: null,
        primerApertura: { $first: "$Apertura" },
        ultimoCierre: { $last: "$Último" },
        totalDias: { $sum: 1 },
        diasPositivos: {
          $sum: { $cond: [{ $gt: ["$Variacion", 0] }, 1, 0] }
        }
      }
    },
    {
      $project: {
        _id: 0,
        rendimientoTotal: {
          $multiply: [
            { $divide: [
              { $subtract: ["$ultimoCierre", "$primerApertura"] },
              "$primerApertura"
            ]},
            100
          ]
        },
        porcentajeDiasPositivos: {
          $multiply: [
            { $divide: ["$diasPositivos", "$totalDias"] },
            100
          ]
        }
      }
    }
  ]
  
  // 6. KPI de Volatilidad Promedio
  [
    {
      $match: {
        "Variacion": { $exists: true },
        "Rango": { $exists: true },
        "Apertura": { $exists: true }
      }
    },
    {
      $group: {
        _id: null,
        volatilidad: { $avg: { $abs: "$Variacion" } },
        rangoPromedio: { $avg: "$Rango" },
        rangoPorcentualPromedio: { 
          $avg: { $divide: ["$Rango", "$Apertura"] } 
        }
      }
    },
    {
      $project: {
        _id: 0,
        volatilidad: { $multiply: ["$volatilidad", 100] },
        rangoPromedio: 1,
        rangoPorcentualPromedio: { $multiply: ["$rangoPorcentualPromedio", 100] }
      }
    }
  ]